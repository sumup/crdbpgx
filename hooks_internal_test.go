package crdbpgx

import (
	"context"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testHooks struct {
	HooksDefaults

	QueryStart      func(context.Context, *pgx.Conn, pgx.TraceQueryStartData) context.Context
	QueryEnd        func(context.Context, *pgx.Conn, pgx.TraceQueryEndData)
	OperationEnd    func(context.Context, OperationEndData)
	NodeSwitchStart func(context.Context, NodeSwitchStartData)
	NodeSwitchEnd   func(context.Context, NodeSwitchEndData)
	RebalanceEnd    func(context.Context, RebalanceEndData)
	HealthUpdate    func(HealthUpdateData)
}

func (h testHooks) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if h.QueryStart != nil {
		return h.QueryStart(ctx, conn, data)
	}
	return ctx
}

func (h testHooks) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if h.QueryEnd != nil {
		h.QueryEnd(ctx, conn, data)
	}
}

func (h testHooks) OnOperationEnd(ctx context.Context, data OperationEndData) {
	if h.OperationEnd != nil {
		h.OperationEnd(ctx, data)
	}
}

func (h testHooks) OnNodeSwitchStart(ctx context.Context, data NodeSwitchStartData) {
	if h.NodeSwitchStart != nil {
		h.NodeSwitchStart(ctx, data)
	}
}

func (h testHooks) OnNodeSwitchEnd(ctx context.Context, data NodeSwitchEndData) {
	if h.NodeSwitchEnd != nil {
		h.NodeSwitchEnd(ctx, data)
	}
}

func (h testHooks) OnRebalanceEnd(ctx context.Context, data RebalanceEndData) {
	if h.RebalanceEnd != nil {
		h.RebalanceEnd(ctx, data)
	}
}

func (h testHooks) OnHealthUpdate(data HealthUpdateData) {
	if h.HealthUpdate != nil {
		h.HealthUpdate(data)
	}
}

// newHookTestConfig uses real pgx connections over in-memory PostgreSQL sessions.
func newHookTestConfig(t *testing.T, node uint32) *pgxpool.Config {
	t.Helper()
	var sessions sync.WaitGroup
	t.Cleanup(sessions.Wait)
	cfg, err := pgxpool.ParseConfig("postgres://test@127.0.0.1/test?sslmode=disable&pool_max_conns=4")
	require.NoError(t, err)
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	cfg.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		sessions.Go(func() {
			defer func() { assert.NoError(t, server.Close()) }()
			backend := pgproto3.NewBackend(server, server)
			if _, err := backend.ReceiveStartupMessage(); !assert.NoError(t, err) {
				return
			}
			backend.Send(&pgproto3.AuthenticationOk{})
			backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "15.0"})
			backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
			backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
			backend.Send(&pgproto3.BackendKeyData{ProcessID: node << 20, SecretKey: []byte{0, 0, 0, 1}})
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if !assert.NoError(t, backend.Flush()) {
				return
			}
			var retries int
			for {
				msg, err := backend.Receive()
				if err != nil {
					return // Pool shutdown or a reset closes the client side.
				}
				switch msg := msg.(type) {
				case *pgproto3.Terminate:
					return
				case *pgproto3.Query:
					if msg.String == "SELECT retry" && retries == 0 {
						retries++
						backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: CrdbRetryErrCode, Message: "retry transaction"})
					} else if msg.String != "" {
						backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
							{Name: []byte("value"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1},
						}})
						backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("1")}})
						backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
					} else {
						backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
					}
					backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					if !assert.NoError(t, backend.Flush()) {
						return
					}
				default:
					t.Errorf("unexpected PostgreSQL message: %T", msg)
					return
				}
			}
		})
		return client, nil
	}
	return cfg
}

func newHookTestPool(t *testing.T, hook Hook, node uint32, opts ...Option) *Pool {
	t.Helper()
	cfg := newHookTestConfig(t, node)
	options := []Option{WithName("orders"), WithHooks(hook), WithConnectRate(time.Nanosecond), WithMaxRetries(1)}
	pool, err := NewPool(t.Context(), cfg, append(options, opts...)...)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func TestHooks_OperationEnd(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		failures int
		reset    bool
	}{
		{name: "success"},
		{name: "serialization retry", failures: 1},
		{name: "retry limit", failures: 2},
		{name: "connection reset", failures: 1, reset: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got []OperationEndData
			type key struct{}
			ctx := context.WithValue(t.Context(), key{}, "caller")
			pool := newHookTestPool(t, testHooks{OperationEnd: func(ctx context.Context, data OperationEndData) {
				assert.Equal(t, "caller", ctx.Value(key{}))
				got = append(got, data)
			}}, 1)
			var attempts int
			err := pool.withRetries(ctx, func(*pgxpool.Conn) error {
				attempts++
				if attempts <= tc.failures {
					if tc.reset {
						return &ResettableError{Err: assert.AnError}
					}
					return &pgconn.PgError{Code: CrdbRetryErrCode}
				}
				return nil
			})
			require.Len(t, got, 1)
			assert.Equal(t, "orders", got[0].PoolName)
			assert.Equal(t, min(tc.failures+1, 2), got[0].Attempts)
			assert.Equal(t, got[0].Attempts-1, got[0].Retries)
			assert.Equal(t, tc.failures == 2, got[0].RetryLimitReached)
			assert.Equal(t, err, got[0].Err)
			if tc.failures == 2 {
				var retryErr *MaxRetryError
				assert.ErrorAs(t, err, &retryErr)
			} else {
				require.NoError(t, err)
			}
			if tc.reset {
				assert.Equal(t, 1, got[0].Resets)
			} else {
				assert.Zero(t, got[0].Resets)
			}
		})
	}

	t.Run("acquisition failure", func(t *testing.T) {
		t.Parallel()
		var got []OperationEndData
		pool := newHookTestPool(t, testHooks{OperationEnd: func(_ context.Context, data OperationEndData) {
			got = append(got, data)
		}}, 1)
		pool.Close()
		err := pool.withRetries(t.Context(), func(*pgxpool.Conn) error {
			t.Fatal("operation ran without acquiring a connection")
			return nil
		})
		require.Error(t, err)
		require.Len(t, got, 1)
		assert.Zero(t, got[0].Attempts)
		assert.Equal(t, err, got[0].Err)
	})
}

func TestHooks_NodeSwitch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		reused bool
		closed bool
	}{
		{name: "different node"},
		{name: "single healthy node", reused: true},
		{name: "acquisition failure", closed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var events []string
			var end NodeSwitchEndData
			pool := newHookTestPool(t, testHooks{
				NodeSwitchStart: func(_ context.Context, data NodeSwitchStartData) {
					events = append(events, "start")
					assert.Equal(t, NodeSwitchStartData{PoolName: "orders", FromNodeID: 1}, data)
				},
				NodeSwitchEnd: func(_ context.Context, data NodeSwitchEndData) {
					events = append(events, "end")
					end = data
				},
			}, 2)
			if !tc.reused {
				pool.health.SetStatus(1, true)
				pool.health.SetStatus(2, true)
			}
			if tc.closed {
				pool.Close()
			}
			conn, err := pool.acquireFromDifferentNode(t.Context(), 1)
			if conn != nil {
				conn.Release()
			}
			assert.Equal(t, []string{"start", "end"}, events)
			assert.Equal(t, "orders", end.PoolName)
			assert.Equal(t, uint32(1), end.FromNodeID)
			assert.Equal(t, tc.reused, end.Reused)
			assert.Equal(t, err, end.Err)
			if tc.closed {
				require.Error(t, err)
				assert.Zero(t, end.ToNodeID)
			} else {
				require.NoError(t, err)
				assert.Equal(t, uint32(2), end.ToNodeID)
			}
		})
	}
}

func TestHooks_RebalanceEnd(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		balancer := NewNodeConnectionBalancer().(*nodeConnectionBalancer)
		t.Cleanup(balancer.ticker.Stop)
		var got []RebalanceEndData
		pool := newHookTestPool(t, testHooks{RebalanceEnd: func(_ context.Context, data RebalanceEndData) {
			if assert.True(t, balancer.mu.TryLock(), "balancer is unlocked before the hook") {
				balancer.mu.Unlock()
			}
			got = append(got, data)
		}}, 1)
		var conns []*pgxpool.Conn
		for range 4 {
			conn, err := pool.Acquire(t.Context())
			require.NoError(t, err)
			conns = append(conns, conn)
		}
		for _, conn := range conns {
			conn.Release()
		}
		synctest.Wait() // pgx completes AfterRelease asynchronously.
		pool.health.SetStatus(2, true)
		balancer.Rebalance(t.Context(), pool)
		assert.Equal(t, []RebalanceEndData{{PoolName: "orders", Acquired: 4, Pruned: 1}}, got)
	})
}

func TestHooks_HealthUpdate(t *testing.T) {
	t.Parallel()
	var got []HealthUpdateData
	var pool *Pool
	pool = newHookTestPool(t, testHooks{HealthUpdate: func(data HealthUpdateData) {
		tracker := pool.health.(*nodeHealthTracker)
		// Reading tracker state verifies that the callback runs without its lock.
		assert.Equal(t, data.HealthyNodes, tracker.NumHealthy())
		assert.Equal(t, data.Healthy, tracker.IsHealthy(data.NodeID))
		if assert.True(t, pool.mu.TryLock(), "pool is unlocked before the hook") {
			pool.mu.Unlock()
		}
		got = append(got, data)
	}}, 1)
	tracker := pool.health.(*nodeHealthTracker)
	conn, err := pool.Acquire(t.Context())
	require.NoError(t, err)
	conn.Release()
	for range 3 {
		tracker.SetStatus(1, false)
	}
	assert.Equal(t, []HealthUpdateData{
		{NodeID: 1, Healthy: true, HealthyNodes: 1},
		{NodeID: 1, Healthy: true, HealthyNodes: 1},
		{NodeID: 1, Healthy: true, HealthyNodes: 1},
		{NodeID: 1, Healthy: false, HealthyNodes: 0},
	}, got)
}

func TestHooks_SharedHealthTracker(t *testing.T) {
	t.Parallel()
	var updates, secondUpdates int
	tracker, err := NewNodeHealthTracker(&pgx.ConnConfig{}, WithHealthTrackerHooks(nil, testHooks{
		HealthUpdate: func(HealthUpdateData) { updates++ },
	}, nil), WithHealthTrackerHooks(nil, testHooks{
		HealthUpdate: func(HealthUpdateData) { secondUpdates++ },
	}, nil))
	require.NoError(t, err)
	cfg, err := pgxpool.ParseConfig("postgres://test@localhost/test")
	require.NoError(t, err)
	for range 2 {
		pool, err := NewPool(t.Context(), cfg, WithHealthTracker(tracker), WithHooks(testHooks{
			HealthUpdate: func(HealthUpdateData) { t.Error("pool replaced shared tracker hooks") },
		}))
		require.NoError(t, err)
		t.Cleanup(pool.Close)
	}
	tracker.SetStatus(1, true)
	assert.Equal(t, 1, updates)
	assert.Equal(t, 1, secondUpdates)
}

type operationEndHook struct {
	HooksDefaults
	callback func(context.Context, OperationEndData)
}

func (h operationEndHook) OnOperationEnd(ctx context.Context, data OperationEndData) {
	h.callback(ctx, data)
}

func TestHooks_Composition(t *testing.T) {
	t.Parallel()
	seen := make(map[string][]string)
	observer := func(name string) testHooks {
		return testHooks{
			OperationEnd:    func(context.Context, OperationEndData) { seen["operation"] = append(seen["operation"], name) },
			NodeSwitchStart: func(context.Context, NodeSwitchStartData) { seen["switch start"] = append(seen["switch start"], name) },
			NodeSwitchEnd:   func(context.Context, NodeSwitchEndData) { seen["switch end"] = append(seen["switch end"], name) },
			RebalanceEnd:    func(context.Context, RebalanceEndData) { seen["rebalance"] = append(seen["rebalance"], name) },
			HealthUpdate:    func(HealthUpdateData) { seen["health"] = append(seen["health"], name) },
		}
	}
	cfg := defaultPoolCfg()
	WithHooks(observer("first"), nil, observer("second"), operationEndHook{
		callback: func(context.Context, OperationEndData) {
			seen["operation"] = append(seen["operation"], "operation only")
		},
	}, HooksDefaults{})(cfg)
	WithHooks(nil, observer("third"), nil)(cfg)
	cfg.hooks.onOperationEnd(t.Context(), OperationEndData{})
	cfg.hooks.onNodeSwitchStart(t.Context(), NodeSwitchStartData{})
	cfg.hooks.onNodeSwitchEnd(t.Context(), NodeSwitchEndData{})
	cfg.hooks.onRebalanceEnd(t.Context(), RebalanceEndData{})
	cfg.hooks.onHealthUpdate(HealthUpdateData{})
	assert.Equal(t, []string{"first", "second", "operation only", "third"}, seen["operation"])
	for _, event := range []string{"switch start", "switch end", "rebalance", "health"} {
		assert.Equal(t, []string{"first", "second", "third"}, seen[event], event)
	}
}

func TestHooks_NilOnly(t *testing.T) {
	t.Parallel()
	pool := newHookTestPool(t, nil, 1)
	require.NoError(t, pool.withRetries(t.Context(), func(*pgxpool.Conn) error { return nil }))
	tracker, err := NewNodeHealthTracker(&pgx.ConnConfig{}, WithHealthTrackerHooks(nil))
	require.NoError(t, err)
	tracker.SetStatus(1, true)
	assert.Equal(t, 1, tracker.NumHealthy())
}

type testConnectTracer struct {
	testHooks
	starts, ends int
}

func (t *testConnectTracer) TraceConnectStart(ctx context.Context, _ pgx.TraceConnectStartData) context.Context {
	t.starts++
	return ctx
}

func (t *testConnectTracer) TraceConnectEnd(context.Context, pgx.TraceConnectEndData) {
	t.ends++
}

func TestHooks_QueryTracing(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		type contextKey struct{}
		var events []string
		observer := func(name, previous string) testHooks {
			return testHooks{
				QueryStart: func(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
					if data.SQL == "" {
						return ctx // pgx may ping an idle connection.
					}
					assert.NotNil(t, conn)
					assert.Equal(t, previous, ctx.Value(contextKey{}))
					events = append(events, name+" start")
					return context.WithValue(ctx, contextKey{}, name)
				},
				QueryEnd: func(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
					if ctx.Value(contextKey{}) != "existing" {
						return // Ignore idle-connection pings.
					}
					events = append(events, name+" end")
				},
			}
		}
		cfg := newHookTestConfig(t, 1)
		cfg.MaxConns = 1
		existing := &testConnectTracer{testHooks: observer("existing", "second")}
		cfg.ConnConfig.Tracer = existing
		first := observer("first", "caller")
		var operations []OperationEndData
		first.OperationEnd = func(_ context.Context, data OperationEndData) {
			operations = append(operations, data)
		}
		pool, err := NewPool(t.Context(), cfg,
			WithHooks(first), WithHooks(nil, observer("second", "first")), WithMaxRetries(1),
		)
		require.NoError(t, err)
		t.Cleanup(pool.Close)
		assert.Same(t, existing, cfg.ConnConfig.Tracer, "the caller's config is unchanged")
		ctx := context.WithValue(t.Context(), contextKey{}, "caller")
		require.NoError(t, pool.ExecFunc(ctx, func(_ context.Context, _ pgconn.CommandTag, err error) error {
			return err
		}, "SELECT 1"))
		assert.Equal(t, []string{
			"first start", "second start", "existing start",
			"first end", "second end", "existing end",
		}, events)

		events = nil
		require.NoError(t, pool.QueryFunc(ctx, func(_ context.Context, rows pgx.Rows) error {
			assert.Equal(t, []string{"first start", "second start", "existing start"}, events)
			rows.Close()
			assert.Len(t, events, 6, "query end runs when rows close")
			return rows.Err()
		}, "SELECT 1"))

		events = nil
		require.NoError(t, pool.QueryRowFunc(ctx, func(_ context.Context, row pgx.Row) error {
			assert.Len(t, events, 3, "QueryRow has not completed before Scan")
			var value int
			err := row.Scan(&value)
			assert.Equal(t, 1, value)
			assert.Len(t, events, 6)
			return err
		}, "SELECT 1"))

		events = nil
		require.NoError(t, pool.AcquireFunc(ctx, func(conn *pgxpool.Conn) error {
			_, err := conn.Exec(ctx, "SELECT 1")
			return err
		}))
		assert.Len(t, events, 6, "directly acquired connections also invoke query hooks")

		events = nil
		require.NoError(t, pool.ExecFunc(ctx, func(_ context.Context, _ pgconn.CommandTag, err error) error {
			return err
		}, "SELECT retry"))
		assert.Len(t, events, 12, "each query attempt invokes start and end on all tracers")
		require.Len(t, operations, 4, "one completion per pool operation")
		assert.Equal(t, 2, operations[3].Attempts)
		assert.Equal(t, 1, operations[3].Retries)
		assert.Equal(t, 1, existing.starts, "the existing connect tracer is preserved")
		assert.Equal(t, 1, existing.ends)
	})
}
