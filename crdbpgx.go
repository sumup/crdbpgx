// Package crdbpgx implements a retrying CockroachDB connection pool wrapper around pgxpool.Pool.
//
// It implements the mechanisms described in the blog post by Authzed:
// https://authzed.com/blog/maximizing-cockroachdb-performance
//
// The package provides automatic retry logic for CockroachDB-specific errors,
// connection rebalancing across nodes, and health tracking for improved reliability
// and performance in distributed CockroachDB deployments.
package crdbpgx
