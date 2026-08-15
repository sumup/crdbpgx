// Package crdbotel provides OpenTelemetry metrics and tracing for crdbpgx.
//
// Tracer implements both crdbpgx's CockroachDB lifecycle tracer and pgx's
// native tracing interfaces. Pass the same tracer to crdbpgx.WithTracer; NewPool
// installs it as the pgx query tracer when the pgx configuration does not already
// have one.
package crdbotel
