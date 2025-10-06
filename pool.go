package crdbpgx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"
)

var (
	meter          = otel.Meter("github.com/sumup/crdbpgx")
	resetHistogram metric.Int64Histogram
	balancerMeter  *balancerMetrics
)

type Pool struct {
	pool   *pgxpool.Pool
	name   string
	health *NodeHealthTracker

	mu          sync.RWMutex
	maxRetries  int
	nodeForConn map[*pgx.Conn]uint32
	gc          map[*pgx.Conn]struct{}

	ctxPropagation ContextPropagationBehaviour
}

type poolCfg struct {
	name           string
	maxRetries     int
	connectRate    time.Duration
	ctxPropagation ContextPropagationBehaviour
}

func NewPool(
	ctx context.Context,
	pgxConfig *pgxpool.Config,
	healthTracker *NodeHealthTracker,
	opts ...Opt,
) (_ *Pool, err error) {
	cfg := defaultPoolCfg()
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.maxRetries < 0 {
		return nil, fmt.Errorf("maxRetries must be greater than or equal to 0 (got: %d)", cfg.maxRetries)
	}

	if resetHistogram == nil {
		resetHistogram, err = meter.Int64Histogram(
			"crdbpgx.resets",
			metric.WithDescription("CockroachDB client-side transaction reset distribution"),
			metric.WithExplicitBucketBoundaries(0, 1, 2, 5, 10, 20, 50),
		)
		if err != nil {
			return nil, fmt.Errorf("initialize crdb client resets histogram: %w", err)
		}
	}
	if balancerMeter == nil {
		balancerMeter, err = newBalancerMetrics()
		if err != nil {
			return nil, err
		}
	}

	pgxConfig = pgxConfig.Copy()

	p := &Pool{
		name:           cfg.name,
		health:         healthTracker,
		maxRetries:     cfg.maxRetries,
		ctxPropagation: cfg.ctxPropagation,
		nodeForConn:    make(map[*pgx.Conn]uint32, 0),
		gc:             make(map[*pgx.Conn]struct{}, 0),
	}

	limiter := rate.NewLimiter(rate.Every(cfg.connectRate), 1)

	afterConnect := pgxConfig.AfterConnect
	pgxConfig.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		// Execute afterConnect hook if passed before
		if afterConnect != nil {
			if err := afterConnect(ctx, c); err != nil {
				return err
			}
		}

		p.mu.Lock()
		defer p.mu.Unlock()

		delete(p.nodeForConn, c)
		delete(p.gc, c)
		healthTracker.SetNodeStatus(nodeID(c), true)

		if err := limiter.Wait(ctx); err != nil {
			return err
		}
		p.nodeForConn[c] = nodeID(c)
		return nil
	}

	// if we attempt to acquire or release a connection that has been marked for
	// GC, return false to tell the underlying pool to destroy the connection
	pruneConnectionIfMarked := func(conn *pgx.Conn) bool {
		p.mu.RLock()
		_, markedForGC := p.gc[conn]
		p.mu.RUnlock()

		if markedForGC {
			p.mu.Lock()
			defer p.mu.Unlock()
			delete(p.gc, conn)
			delete(p.nodeForConn, conn)
		}
		return !markedForGC
	}

	prepareConn := pgxConfig.PrepareConn
	pgxConfig.PrepareConn = func(ctx context.Context, c *pgx.Conn) (bool, error) {
		if prepareConn != nil {
			ok, err := prepareConn(ctx, c)
			if !ok || err != nil {
				return ok, err
			}
		}
		return pruneConnectionIfMarked(c), nil
	}

	//nolint:staticcheck // Supporting for migration period
	beforeAcquire := pgxConfig.BeforeAcquire
	//nolint:staticcheck // Supporting for migration period
	pgxConfig.BeforeAcquire = func(ctx context.Context, c *pgx.Conn) bool {
		if beforeAcquire != nil {
			if !beforeAcquire(ctx, c) {
				return false
			}
		}
		return pruneConnectionIfMarked(c)
	}

	afterRelease := pgxConfig.AfterRelease
	pgxConfig.AfterRelease = func(c *pgx.Conn) bool {
		if afterRelease != nil {
			if !afterRelease(c) {
				return false
			}
		}
		return pruneConnectionIfMarked(c)
	}

	beforeClose := pgxConfig.BeforeClose
	pgxConfig.BeforeClose = func(c *pgx.Conn) {
		if beforeClose != nil {
			beforeClose(c)
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		delete(p.nodeForConn, c)
		delete(p.gc, c)
	}

	// Finally, we create the actual connection pool
	p.pool, err = pgxpool.NewWithConfig(ctx, pgxConfig)
	if err != nil {
		return nil, fmt.Errorf("create crdbpgx.Pool from config: %w", err)
	}
	return p, nil
}

// Returns the name of the pool.
func (p *Pool) Name() string {
	return p.name
}

// Returns the maximum number of connections on the underlying pool.
func (p *Pool) MaxConns() int {
	return int(p.pool.Config().MaxConns)
}

func (p *Pool) MinConns() int {
	return int(p.pool.Config().MinConns)
}

func (p *Pool) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	ctx = p.contextForAcquire(ctx)
	return p.acquire(ctx)
}

func (p *Pool) AcquireAllIdle(ctx context.Context) []*pgxpool.Conn {
	ctx = p.contextForAcquire(ctx)
	return p.pool.AcquireAllIdle(ctx)
}

func (p *Pool) AcquireFunc(ctx context.Context, f func(*pgxpool.Conn) error) error {
	ctx = p.contextForAcquire(ctx)
	conn, err := p.acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	return f(conn)
}

func (p *Pool) acquire(ctx context.Context) (*pgxpool.Conn, error) {
	// pgxpool performs a healthcheck after acquiring a connection that has been in idle state for longer
	// than one second. This healthcheck involves a Ping operation. If the context is canceled on Acquire,
	// after the connection has been taken from the pool, but before the healthcheck concluded, this
	// counts as a cancellation of a database operation and results in the destroy of the connection.
	//
	// Until pgx supports configurable healthcheck behavior, we send context for database operations
	// to the acquire function to prevent this from happening. If the context for acquire is canceled,
	// we check this right after concluding the acquire and put it directly back to the pool.
	//
	// See also:
	// - https://github.com/jackc/pgx/issues/1738
	// - https://github.com/jackc/pgx/blob/58b05f567c01eafb14218f659a16a5384ffe5dfa/pgxpool/pool.go#L533
	conn, err := p.pool.Acquire(p.contextForDBOp(ctx))
	if err != nil {
		return nil, err
	}

	if ctx.Err() != nil {
		conn.Release()
		return nil, ctx.Err()
	}

	return conn, nil
}

func (p *Pool) Ping(ctx context.Context) error {
	return p.pool.Ping(p.contextForDBOp(ctx))
}

// Tx represents a CRDB database transaction.
//
// Tx can is used in place of [pgx.Tx] and provides additional methods that are also defined
// on the [Pool] such that users can use the same set of functions both outside and inside
// a transaction.
type Tx interface {
	pgx.Tx
	ExecFunc(ctx context.Context, tagFunc func(context.Context, pgconn.CommandTag, error) error, sql string, arguments ...any) error
	QueryFunc(ctx context.Context, rowsFunc func(context.Context, pgx.Rows) error, sql string, optionsAndArgs ...any) error
	QueryRowFunc(ctx context.Context, rowFunc func(context.Context, pgx.Row) error, sql string, optionsAndArgs ...any) error
}

// InTxFunc is a replacement for `pgxpool.BeginTxFunc` that resets the connection on resettable errors
// and retries the transaction on retryable errors.
func (p *Pool) InTxFunc(ctx context.Context, txOptions pgx.TxOptions, txFunc func(Tx) error) error {
	return p.withRetries(ctx, func(conn *pgxpool.Conn) error {
		ctx = p.contextForDBOp(ctx)
		tx, err := conn.BeginTx(ctx, txOptions)
		if err != nil {
			return err
		}
		return beginFuncExec(ctx, crdbTx{tx}, txFunc)
	})
}

type crdbTx struct{ pgx.Tx }

func (p *Pool) Close() {
	p.pool.Close()
}

func (p *Pool) Config() *pgxpool.Config {
	return p.pool.Config().Copy()
}

func (p *Pool) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	ctx = p.contextForAcquire(ctx)
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	ctx = p.contextForDBOp(ctx)
	return p.pool.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (p *Pool) ExecFunc(ctx context.Context, tagFunc func(ctx context.Context, tag pgconn.CommandTag, err error) error, sql string, arguments ...any) error {
	return p.withRetries(ctx, func(conn *pgxpool.Conn) error {
		ctx = p.contextForDBOp(ctx)
		tag, err := conn.Conn().Exec(ctx, sql, arguments...)
		return tagFunc(ctx, tag, err)
	})
}

func (t crdbTx) ExecFunc(ctx context.Context, tagFunc func(ctx context.Context, tag pgconn.CommandTag, err error) error, sql string, arguments ...any) error {
	tag, err := t.Exec(ctx, sql, arguments...)
	return tagFunc(ctx, tag, err)
}

func (p *Pool) QueryFunc(ctx context.Context, rowsFunc func(ctx context.Context, rows pgx.Rows) error, sql string, optionsAndArgs ...any) error {
	return p.withRetries(ctx, func(conn *pgxpool.Conn) error {
		ctx = p.contextForDBOp(ctx)
		rows, err := conn.Conn().Query(ctx, sql, optionsAndArgs...)
		if err != nil {
			return err
		}
		defer rows.Close()
		return rowsFunc(ctx, rows)
	})
}

func (t crdbTx) QueryFunc(ctx context.Context, rowsFunc func(ctx context.Context, rows pgx.Rows) error, sql string, optionsAndArgs ...any) error {
	rows, err := t.Query(ctx, sql, optionsAndArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return rowsFunc(ctx, rows)
}

func (p *Pool) QueryRowFunc(ctx context.Context, rowFunc func(ctx context.Context, row pgx.Row) error, sql string, optionsAndArgs ...any) error {
	return p.withRetries(ctx, func(conn *pgxpool.Conn) error {
		ctx = p.contextForDBOp(ctx)
		return rowFunc(ctx, conn.Conn().QueryRow(ctx, sql, optionsAndArgs...))
	})
}

func (t crdbTx) QueryRowFunc(ctx context.Context, rowFunc func(ctx context.Context, row pgx.Row) error, sql string, optionsAndArgs ...any) error {
	row := t.QueryRow(ctx, sql, optionsAndArgs...)
	return rowFunc(ctx, row)
}

func (p *Pool) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return p.pool.SendBatch(p.contextForDBOp(ctx), b)
}

func (p *Pool) Stat() *pgxpool.Stat {
	return p.pool.Stat()
}

// NodeID returns the ID of the node a connection uses.
func (p *Pool) NodeID(conn *pgx.Conn) uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	id, ok := p.nodeForConn[conn]
	if !ok {
		return 0
	}
	return id
}

// GCMark marks a connection to be destroyed on the next acquire.
func (p *Pool) GCMark(conn *pgx.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gc[conn] = struct{}{}
	delete(p.nodeForConn, conn)
}

// connsPerNode count the active connection per nodeID.
func (p *Pool) connsPerNode() map[uint32]int {
	counts := make(map[uint32]int)

	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, nid := range p.nodeForConn {
		counts[nid]++
	}

	return counts
}

func (p *Pool) acquireFromDifferentNode(ctx context.Context, currentNodeID uint32) (*pgxpool.Conn, error) {
	slog.DebugContext(ctx, "crdbpgx: acquiring connection from a different node", slog.Uint64("current_node_id", uint64(currentNodeID)))
	span := trace.SpanFromContext(ctx)
	span.AddEvent("acquiring connection from different node", trace.WithAttributes(
		attribute.Int64("current_node_id", int64(currentNodeID)),
	))

	for {
		conn, err := p.pool.Acquire(ctx)
		if err != nil {
			if conn != nil {
				conn.Release()
			}
			return nil, err
		}

		if p.health.NumHealthyNodes() <= 1 {
			slog.DebugContext(ctx, "crdbpgx: only one healthy node available, reuse connection", slog.Uint64("current_node_id", uint64(currentNodeID)))
			span.AddEvent("only one healthy node available, reuse connection", trace.WithAttributes(
				attribute.Int64("current_node_id", int64(currentNodeID)),
			))
			return conn, nil
		}

		id := p.NodeID(conn.Conn())
		if id == currentNodeID {
			conn.Release()
			continue
		}

		slog.DebugContext(ctx, "crdbpgx: acquired connection from different node", slog.Uint64("node_id", uint64(id)))
		span.AddEvent("acquired connection from different node", trace.WithAttributes(attribute.Int64("node_id", int64(id))))
		return conn, nil
	}
}

func (p *Pool) withRetries(ctx context.Context, fn func(conn *pgxpool.Conn) error) error {
	conn, err := p.Acquire(ctx)
	if err != nil {
		if conn != nil {
			conn.Release()
		}
		return fmt.Errorf("acquire connection from pool: %w", err)
	}

	var retries, maxRetries uint
	defer func() {
		if conn != nil {
			conn.Release()
		}
		resetHistogram.Record(
			context.Background(),
			int64(retries), //nolint:gosec // retries are known to stay within a reasonable range
			metric.WithAttributes(
				attribute.String("pool.id", p.Name()),
			),
		)
	}()

	if !RetriesDisabled(ctx) {
		if mr := p.maxRetries; mr > 0 {
			maxRetries = uint(mr)
		}
	}

	for retries = 0; retries <= maxRetries; retries++ {
		err = wrapSQLError(ctx, fn(conn))
		if err == nil {
			conn.Release()
			if retries > 0 {
				slog.DebugContext(ctx, "ctdbpgx: successful query after retries", slog.Uint64("retries", uint64(retries)))
			}
			return nil
		}

		// Handle errors
		var (
			resettableErr *ResettableError
			retryableErr  *RetryableError
		)
		if errors.As(err, &resettableErr) || conn.Conn().IsClosed() {
			slog.DebugContext(ctx, "crdbpgx: resetting connection", slog.Uint64("retries", uint64(retries)))

			nodeID := p.NodeID(conn.Conn())
			p.GCMark(conn.Conn())
			conn.Release()

			// If a resettable error occurred, we mark the node as unhealthy.
			if nodeID > 0 {
				p.health.SetNodeStatus(nodeID, false)
			}

			sleepOnErr(ctx, 0)

			conn, err = p.acquireFromDifferentNode(ctx, nodeID)
			if err != nil {
				return fmt.Errorf("acquire connection from poll after retry %d: %w", retries, err)
			}
			continue
		} else if errors.As(err, &retryableErr) {
			slog.DebugContext(ctx, "crdbpgx: retryable error", slog.Uint64("retries", uint64(retries)), slog.Any("error", err))
			sleepOnErr(ctx, retries)
			continue
		}

		// The error is not resettable or retry-able, return it
		conn.Release()

		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			slog.DebugContext(
				ctx,
				"crdbpgx: non-retryable error",
				slog.Uint64("retries", uint64(retries)),
				slog.Any("error", err),
			)
		}

		return err
	}
	// If we arrive here, the maximum retries threshold have been reached
	return &MaxRetryError{
		MaxRetries: maxRetries,
		LastErr:    err,
	}
}

func sleepOnErr(ctx context.Context, retries uint) {
	after := exponentialBackoffWithJitter(25*time.Millisecond, 0.6)(ctx, retries+1)
	slog.DebugContext(ctx, "crdbpgx: sleeping before retry", slog.Uint64("retries", uint64(retries)), slog.Duration("sleep_duration", after))

	select {
	case <-time.After(after):
	case <-ctx.Done():
	}
}
