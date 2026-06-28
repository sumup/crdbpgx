// Package crdbpgx provides a CockroachDB-aware wrapper around
// [github.com/jackc/pgx/v5/pgxpool].
//
// It is intended for applications that connect to CockroachDB with pgx and want
// pool-level behavior that is specific to CockroachDB clusters: automatic retry
// handling for retryable transaction errors, connection reset and reacquisition
// for node-level failures, node health tracking, and optional connection
// rebalancing across CockroachDB gateway nodes.
//
// The package is inspired by the mechanisms described in AuthZed's article
// "Maximizing CockroachDB Performance: Our Journey to 1 Million QPS":
// https://authzed.com/blog/maximizing-cockroachdb-performance
//
// # Pool lifecycle
//
// Create a Pool with [NewPool] from a [pgxpool.Config]. The context passed to
// [NewPool] is used only while the underlying pgx pool is created. Runtime
// background work started by this package, such as the default health tracker
// and the optional node connection balancer, is owned by the Pool and is stopped
// by [Pool.Close].
//
// Always close the pool when it is no longer needed:
//
//	defer pool.Close()
//
// [Pool.Close] starts shutdown once and waits for internal goroutines and the
// underlying pgx pool to finish closing.
//
// # Context propagation
//
// Database operation contexts can affect connection health in pgx: canceling a
// context while a query is active can leave the connection unusable. crdbpgx
// exposes [WithContextPropagationBehaviour] to control how caller contexts are
// propagated to connection acquisition and database operations.
//
// The default, [ContextPropagationDetach], detaches both acquisition and
// database operations from caller cancellation. [ContextPropagationOnAcquire]
// keeps caller cancellation for acquiring a connection, but detaches the actual
// database operation. [ContextPropagationPassthrough] passes caller contexts
// directly to pgx.
//
// # Example: create a pool
//
//	cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
//	if err != nil {
//		return fmt.Errorf("parse database URL: %w", err)
//	}
//
//	pool, err := crdbpgx.NewPool(
//		context.Background(),
//		cfg,
//		crdbpgx.WithName("orders"),
//		crdbpgx.WithContextPropagationBehaviour(crdbpgx.ContextPropagationOnAcquire),
//		crdbpgx.WithBalancer(crdbpgx.NewNodeConnectionBalancer(
//			crdbpgx.WithBalancingInterval(30*time.Second),
//			crdbpgx.WithBalancingTimeout(10*time.Second),
//		)),
//	)
//	if err != nil {
//		return fmt.Errorf("create pool: %w", err)
//	}
//	defer pool.Close()
//
// # Example: run a transaction with automatic retries
//
//	err := pool.InTx(ctx, pgx.TxOptions{}, func(ctx context.Context, db crdbpgx.Runner) error {
//		return db.ExecFunc(ctx, func(_ context.Context, _ pgconn.CommandTag, err error) error {
//			return err
//		}, "UPDATE accounts SET balance = balance - $1 WHERE id = $2", amount, accountID)
//	})
//	if err != nil {
//		return fmt.Errorf("transfer funds: %w", err)
//	}
package crdbpgx
