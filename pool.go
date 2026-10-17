package crdbpgx

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/time/rate"
)

// Pool is a CockroachDB-aware connection pool that wraps [pgxpool.Pool] with
// automatic retry logic, node health tracking, and optional connection
// rebalancing capabilities.
//
// Pool provides the same interface as [pgxpool.Pool] but adds resilience features
// specifically designed for CockroachDB clusters, including automatic handling of
// retryable and resettable errors, and optional connection management across nodes.
type Pool struct {
	// Runner provides typed queries and transactions backed by this pool.
	Runner

	// pool is the wrapped pgx connection pool.
	pool *pgxpool.Pool

	// name identifies the pool in logs and metrics.
	name string

	// health tracks CockroachDB node health.
	health NodeHealthTracker

	// mu protects connection tracking and GC state.
	mu sync.RWMutex

	// maxRetries is the retry limit for database operations.
	maxRetries int

	// nodeForConn maps live connections to CockroachDB node IDs.
	nodeForConn map[*pgx.Conn]uint32

	// gc marks connections that should be pruned by pgxpool.
	gc map[*pgx.Conn]struct{}

	// ctxPropagation controls how caller cancellation reaches database work.
	ctxPropagation ContextPropagationBehaviour

	// logger emits optional debug logs.
	logger *slog.Logger

	// onShutdown contains callbacks run during Pool.Close.
	onShutdown []func()

	// shutdownWG waits for pool-owned background goroutines.
	shutdownWG *sync.WaitGroup

	// closeOnce makes Pool.Close idempotent.
	closeOnce sync.Once

	// hooks observes pool lifecycle events.
	hooks hooks

	// connectLimiter applies a rate limit for connection attempts
	connectLimiter *rate.Limiter
}

var (
	_ Executor = (*Pool)(nil)
)

// NewPool creates a new CockroachDB-aware connection pool.
//
// The pool wraps [pgxpool.Pool] and adds CockroachDB-specific features including
// automatic retry logic, node health tracking, and optional connection rebalancing.
//
// NewPool copies pgxConfig before applying options. ctx controls initialization;
// the pool stops its background work when [Pool.Close] is called.
// Use [WithHealthTracker] to share a caller-owned health tracker across pools.
// NewPool returns an error if the configuration is invalid or pool creation fails.
func NewPool(
	ctx context.Context,
	pgxConfig *pgxpool.Config,
	opts ...Option,
) (_ *Pool, err error) {
	pgxConfig = pgxConfig.Copy()

	cfg := defaultPoolCfg()
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.maxRetries < 0 {
		return nil, fmt.Errorf("maxRetries must be greater than or equal to 0 (got: %d)", cfg.maxRetries)
	}

	if len(cfg.hooks) > 0 {
		tracers := make([]pgx.QueryTracer, 0, len(cfg.hooks)+1)
		for _, hook := range cfg.hooks {
			tracers = append(tracers, hook)
		}
		if pgxConfig.ConnConfig.Tracer != nil {
			tracers = append(tracers, pgxConfig.ConnConfig.Tracer)
		}
		pgxConfig.ConnConfig.Tracer = multitracer.New(tracers...)
	}

	// if no custom health tracker was provided we bootstrap one for this
	// pool automatically
	healthTracker := cfg.healthTracker
	var onShutdown []func()
	shutdownWG := new(sync.WaitGroup)
	if healthTracker == nil {
		nodeHealthTracker, err := NewNodeHealthTracker(pgxConfig.ConnConfig, WithHealthTrackerHooks(cfg.hooks...))
		if err != nil {
			return nil, fmt.Errorf("new node health tracker: %w", err)
		}

		healthTrackerCtx, cancel := context.WithCancel(context.Background())
		shutdownWG.Go(func() {
			nodeHealthTracker.Poll(healthTrackerCtx)
		})

		healthTracker = nodeHealthTracker
		onShutdown = append(onShutdown, cancel)
	}

	p := &Pool{
		name:           cfg.name,
		health:         healthTracker,
		maxRetries:     cfg.maxRetries,
		ctxPropagation: cfg.ctxPropagation,
		nodeForConn:    make(map[*pgx.Conn]uint32, 0),
		gc:             make(map[*pgx.Conn]struct{}, 0),
		logger:         cmp.Or(cfg.logger, slog.Default()),
		onShutdown:     onShutdown,
		shutdownWG:     shutdownWG,
		hooks:          cfg.hooks,
		connectLimiter: rate.NewLimiter(rate.Every(cfg.connectRate), 1),
	}

	p.Runner = NewRunner(p)

	afterConnect := pgxConfig.AfterConnect
	pgxConfig.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		return p.afterConnect(ctx, c, afterConnect)
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
		for _, callback := range p.onShutdown {
			callback()
		}
		p.shutdownWG.Wait()
		return nil, fmt.Errorf("create crdbpgx.Pool from config: %w", err)
	}

	for _, hook := range p.hooks {
		if cleanup := hook.OnPoolCreated(p); cleanup != nil {
			p.onShutdown = append(p.onShutdown, cleanup)
		}
	}

	if cfg.balancer != nil {
		balancerCtx, cancel := context.WithCancel(context.Background())
		p.shutdownWG.Go(func() {
			cfg.balancer.Run(balancerCtx, p)
		})
		p.onShutdown = append(p.onShutdown, cancel)
	}

	return p, nil
}

// Name returns the configured name of the pool, used for observability purposes.
func (p *Pool) Name() string {
	return p.name
}

// MaxConns returns the maximum number of connections in the underlying pool.
func (p *Pool) MaxConns() int {
	return int(p.pool.Config().MaxConns)
}

// MinConns returns the minimum number of connections in the underlying pool.
func (p *Pool) MinConns() int {
	return int(p.pool.Config().MinConns)
}

// Acquire returns a connection from the pool. The connection must be released
// back to the pool by calling [pgxpool.Conn.Release] on the returned connection.
func (p *Pool) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	ctx = p.contextForAcquire(ctx)
	return p.acquire(ctx)
}

// AcquireAllIdle atomically acquires all currently idle connections from the pool.
// This is primarily used by the node connection balancer.
func (p *Pool) AcquireAllIdle(ctx context.Context) []*pgxpool.Conn {
	ctx = p.contextForAcquire(ctx)
	return p.pool.AcquireAllIdle(ctx)
}

// AcquireFunc acquires a connection, calls the provided function with it,
// and releases the connection when the function returns.
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
	// than one second. This healthcheck involves a Ping operation. If the context is canceled on [Pool.Acquire],
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

// Ping verifies that a connection to the database is still alive.
func (p *Pool) Ping(ctx context.Context) error {
	return p.pool.Ping(p.contextForDBOp(ctx))
}

// Executor is the callback execution interface implemented by both [Pool] and [Tx].
//
// Use Runner for typed queries and transaction callbacks. Executor is the
// underlying execution seam for adapters and test implementations.
type Executor interface {
	ExecFunc(ctx context.Context, tagFunc func(context.Context, pgconn.CommandTag, error) error, sql string, arguments ...any) error
	QueryFunc(ctx context.Context, rowsFunc func(context.Context, pgx.Rows) error, sql string, optionsAndArgs ...any) error
	QueryRowFunc(ctx context.Context, rowFunc func(context.Context, pgx.Row) error, sql string, optionsAndArgs ...any) error
	// InTxFunc ignores [pgx.TxOptions] when invoked as a nested (pseudo) transaction.
	InTxFunc(context.Context, pgx.TxOptions, func(Tx) error) error
}

// Tx represents a CockroachDB database transaction.
//
// Tx can be used in place of [pgx.Tx] and provides additional methods that are also defined
// on [Pool] such that users can use the same set of functions both outside and inside
// a transaction (ExecFunc, QueryFunc, QueryRowFunc, InTxFunc).
type Tx interface {
	pgx.Tx
	Executor
}

// InTxFunc executes a callback with the underlying transaction.
// It retries the entire callback on retryable errors and resets connections on
// resettable errors. Use Runner.InTx for a callback with typed query methods.
func (p *Pool) InTxFunc(ctx context.Context, txOptions pgx.TxOptions, txFunc func(Tx) error) error {
	return p.withRetries(ctx, func(conn *pgxpool.Conn) error {
		dbCtx := p.contextForDBOp(ctx)
		tx, err := conn.BeginTx(dbCtx, txOptions)
		if err != nil {
			return err
		}
		return beginFuncExec(dbCtx, crdbTx{Tx: tx, dbContext: p.contextForDBOp}, txFunc)
	})
}

func (t crdbTx) InTxFunc(
	ctx context.Context,
	_ pgx.TxOptions,
	fn func(Tx) error,
) error {
	ctx = t.dbContext(ctx)
	nested, err := t.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin savepoint: %w", err)
	}
	return beginFuncExec(ctx, crdbTx{Tx: nested, dbContext: t.dbContext}, fn)
}

type crdbTx struct {
	pgx.Tx
	dbContext func(context.Context) context.Context
}

var (
	_ Executor = crdbTx{}
	_ Tx       = crdbTx{}
)

// Close closes all connections in the pool and prevents new connections from being acquired.
func (p *Pool) Close() {
	p.closeOnce.Do(func() {
		for _, callback := range p.onShutdown {
			callback()
		}

		p.pool.Close()
		p.shutdownWG.Wait()
	})
}

// Config returns a copy of the pool's configuration.
func (p *Pool) Config() *pgxpool.Config {
	return p.pool.Config().Copy()
}

// CopyFrom efficiently copies data from rowSrc to the specified table using the PostgreSQL
// COPY protocol. Note that this operation does not have automatic retry logic.
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

// ExecFunc executes a SQL command and calls tagFunc with the command result.
// The operation includes automatic retry logic for retryable and resettable errors.
func (p *Pool) ExecFunc(ctx context.Context, tagFunc func(ctx context.Context, tag pgconn.CommandTag, err error) error, sql string, arguments ...any) error {
	return p.withRetries(ctx, func(conn *pgxpool.Conn) error {
		ctx = p.contextForDBOp(ctx)
		tag, err := conn.Conn().Exec(ctx, sql, arguments...)
		return tagFunc(ctx, tag, err)
	})
}

// ExecFunc executes a SQL command within the transaction and calls tagFunc with the result.
func (t crdbTx) ExecFunc(ctx context.Context, tagFunc func(ctx context.Context, tag pgconn.CommandTag, err error) error, sql string, arguments ...any) error {
	ctx = t.dbContext(ctx)
	tag, err := t.Exec(ctx, sql, arguments...)
	return tagFunc(ctx, tag, err)
}

// QueryFunc executes a SQL query and calls rowsFunc with the resulting rows.
// The operation includes automatic retry logic for retryable and resettable errors.
// rowsFunc must close the rows when done.
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

// QueryFunc executes a SQL query within the transaction and calls rowsFunc with the resulting rows.
func (t crdbTx) QueryFunc(ctx context.Context, rowsFunc func(ctx context.Context, rows pgx.Rows) error, sql string, optionsAndArgs ...any) error {
	ctx = t.dbContext(ctx)
	rows, err := t.Query(ctx, sql, optionsAndArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return rowsFunc(ctx, rows)
}

// QueryRowFunc executes a SQL query that returns at most one row and calls rowFunc with the result.
// The operation includes automatic retry logic for retryable and resettable errors.
func (p *Pool) QueryRowFunc(ctx context.Context, rowFunc func(ctx context.Context, row pgx.Row) error, sql string, optionsAndArgs ...any) error {
	return p.withRetries(ctx, func(conn *pgxpool.Conn) error {
		ctx = p.contextForDBOp(ctx)
		return rowFunc(ctx, conn.Conn().QueryRow(ctx, sql, optionsAndArgs...))
	})
}

// QueryRowFunc executes a SQL query that returns at most one row within the transaction
// and calls rowFunc with the result.
func (t crdbTx) QueryRowFunc(ctx context.Context, rowFunc func(ctx context.Context, row pgx.Row) error, sql string, optionsAndArgs ...any) error {
	ctx = t.dbContext(ctx)
	row := t.QueryRow(ctx, sql, optionsAndArgs...)
	return rowFunc(ctx, row)
}

// SendBatch sends a batch of queries to the database for execution.
// Note that batch operations do not have automatic retry logic.
func (p *Pool) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return p.pool.SendBatch(p.contextForDBOp(ctx), b)
}

// Stat returns statistics about the connection pool.
func (p *Pool) Stat() *pgxpool.Stat {
	return p.pool.Stat()
}

// NodeID returns the CockroachDB node ID for the given connection.
// Returns 0 if the connection is not tracked.
func (p *Pool) NodeID(conn *pgx.Conn) uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	id, ok := p.nodeForConn[conn]
	if !ok {
		return 0
	}
	return id
}

// GCMark marks a connection to be destroyed on the next pool operation (acquire/release).
// This is used internally for connection rebalancing and when recovering from node failures.
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

type afterConnectFunc func(context.Context, *pgx.Conn) error

func (p *Pool) afterConnect(ctx context.Context, conn *pgx.Conn, wrapped afterConnectFunc) error {
	if wrapped != nil {
		if err := wrapped(ctx, conn); err != nil {
			return err
		}
	}

	if err := p.connectLimiter.Wait(ctx); err != nil {
		return err
	}

	p.health.SetStatus(nodeID(conn), true)

	p.mu.Lock()
	p.nodeForConn[conn] = nodeID(conn)
	delete(p.gc, conn)
	p.mu.Unlock()

	return nil
}

func (p *Pool) acquireFromDifferentNode(ctx context.Context, currentNodeID uint32) (replacement *pgxpool.Conn, err error) {
	data := NodeSwitchEndData{
		PoolName:   p.Name(),
		FromNodeID: currentNodeID,
	}
	p.hooks.onNodeSwitchStart(ctx, NodeSwitchStartData{
		PoolName:   p.Name(),
		FromNodeID: currentNodeID,
	})
	defer func() {
		data.Err = err
		if replacement != nil {
			data.ToNodeID = p.NodeID(replacement.Conn())
		}
		p.hooks.onNodeSwitchEnd(ctx, data)
	}()
	p.logger.DebugContext(ctx, "crdbpgx: acquiring connection from a different node",
		slog.Uint64("current_node_id", uint64(currentNodeID)),
	)

	for {
		conn, err := p.pool.Acquire(ctx)
		if err != nil {
			if conn != nil {
				conn.Release()
			}
			return nil, err
		}

		if p.health.NumHealthy() <= 1 {
			p.logger.DebugContext(ctx, "crdbpgx: only one healthy node available, reuse connection",
				slog.Uint64("current_node_id", uint64(currentNodeID)),
			)

			data.Reused = true
			return conn, nil
		}

		id := p.NodeID(conn.Conn())
		if id == currentNodeID {
			conn.Release()
			continue
		}

		p.logger.DebugContext(ctx, "crdbpgx: acquired connection from different node",
			slog.Uint64("node_id", uint64(id)),
		)

		return conn, nil
	}
}

func (p *Pool) withRetries(ctx context.Context, fn func(conn *pgxpool.Conn) error) (err error) {
	data := OperationEndData{PoolName: p.Name()}
	defer func() {
		data.Retries = max(data.Attempts-1, 0)
		data.Err = err
		p.hooks.onOperationEnd(ctx, data)
	}()
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
	}()

	if !RetriesDisabled(ctx) {
		if mr := p.maxRetries; mr > 0 {
			maxRetries = uint(mr)
		}
	}

	for retries = 0; retries <= maxRetries; retries++ {
		data.Attempts++
		err = wrapSQLError(ctx, fn(conn))
		if err == nil {
			conn.Release()
			if retries > 0 {
				p.logger.DebugContext(ctx, "ctdbpgx: successful query after retries",
					slog.Uint64("retries", uint64(retries)),
				)
			}
			return nil
		}

		// Handle errors
		var (
			resettableErr *ResettableError
			retryableErr  *RetryableError
		)
		if errors.As(err, &resettableErr) || conn.Conn().IsClosed() {
			p.logger.DebugContext(ctx, "crdbpgx: resetting connection", slog.Uint64("retries", uint64(retries)))

			data.Resets++
			nodeID := p.NodeID(conn.Conn())
			p.GCMark(conn.Conn())
			conn.Release()

			// If a resettable error occurred, we mark the node as unhealthy.
			if nodeID > 0 {
				p.health.SetStatus(nodeID, false)
			}

			sleepOnErr(ctx, 0)

			conn, err = p.acquireFromDifferentNode(ctx, nodeID)
			if err != nil {
				return fmt.Errorf("acquire connection from poll after retry %d: %w", retries, err)
			}
			continue
		} else if errors.As(err, &retryableErr) {
			p.logger.DebugContext(ctx, "crdbpgx: retryable error",
				slog.Uint64("retries", uint64(retries)), slog.Any("error", err),
			)
			sleepOnErr(ctx, retries)
			continue
		}

		// The error is not resettable or retry-able, return it
		conn.Release()

		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			p.logger.DebugContext(ctx, "crdbpgx: non-retryable error",
				slog.Uint64("retries", uint64(retries)),
				slog.Any("error", err),
			)
		}

		return err
	}
	data.RetryLimitReached = true
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
