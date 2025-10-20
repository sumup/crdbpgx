package crdbpgx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// CrdbRetryErrCode is the PostgreSQL error code for transaction retry errors in CockroachDB.
	// https://www.cockroachlabs.com/docs/stable/common-errors.html#restart-transaction
	CrdbRetryErrCode = "40001"

	// CrdbAmbiguousErrorCode is the PostgreSQL error code for ambiguous result errors in CockroachDB.
	// https://www.cockroachlabs.com/docs/stable/common-errors.html#result-is-ambiguous
	CrdbAmbiguousErrorCode = "40003"

	// CrdbServerNotAcceptingClients is the PostgreSQL error code when a CockroachDB node is shutting down.
	// https://www.cockroachlabs.com/docs/stable/node-shutdown.html#connection-retry-loop
	CrdbServerNotAcceptingClients = "57P01"

	// CrdbUnknownSQLState is returned when the SQL state cannot be determined.
	CrdbUnknownSQLState = "XXUUU"

	// CrdbClockSkewMessage is the error message when CockroachDB nodes have large clock skew.
	CrdbClockSkewMessage = "cannot specify timestamp in the future"

	errMsgResettable = "resettable error"
	errMsgRetryable  = "retryable error"
)

func wrapSQLError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return err
	}

	if IsResettableError(ctx, err) {
		return &ResettableError{Err: err}
	}
	if IsRetryableError(ctx, err) {
		return &RetryableError{Err: err}
	}

	return err
}

// IsRetryableError returns true if the error is a CockroachDB retryable error.
//
// Retryable errors are serialization conflicts that can be resolved by retrying
// the transaction. The connection remains valid and can be reused.
func IsRetryableError(ctx context.Context, err error) bool {
	sqlState := sqlErrorCode(err)
	if sqlState == "" {
		slog.DebugContext(ctx, "could not determine a sqlstate error code", slog.Any("error", err))
	}

	switch sqlState {
	case CrdbRetryErrCode:
		return true
	case CrdbUnknownSQLState:
		return strings.Contains(err.Error(), CrdbClockSkewMessage)
	default:
		return false
	}
}

// IsResettableError returns true if the error indicates that the connection
// should be reset and a retry attempted on a different node.
//
// Resettable errors typically indicate node failures, network issues, or
// ambiguous transaction results. The connection is no longer usable and
// should be replaced.
func IsResettableError(ctx context.Context, err error) bool {
	// We check if the error is likely caused by an error that would occur if a node is taken offline
	errMsg := err.Error()
	if strings.Contains(errMsg, "broken pipe") ||
		strings.Contains(errMsg, "unexpected EOF") ||
		strings.Contains(errMsg, "conn closed") ||
		strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "connection reset by peer") {
		return true
	}

	sqlState := sqlErrorCode(err)
	if sqlState == "" {
		slog.DebugContext(ctx, "could not determine a sqlstate error code", slog.Any("error", err))
	}

	switch sqlState {
	case CrdbAmbiguousErrorCode, CrdbServerNotAcceptingClients:
		return true
	default:
		return false
	}
}

func sqlErrorCode(err error) string {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return ""
	}
	return pgErr.SQLState()
}

// MaxRetryError is returned when the maximum number of retries has been exceeded.
type MaxRetryError struct {
	MaxRetries uint
	LastErr    error
}

func (e *MaxRetryError) Error() string {
	if e.MaxRetries == 0 {
		if e.LastErr != nil {
			return "retries disabled: " + e.LastErr.Error()
		}
		return "retries disabled"
	}

	if e.LastErr == nil {
		return fmt.Sprintf("max retries reached (%d)", e.MaxRetries)
	}

	return fmt.Sprintf("max retries reached (%d): %s", e.MaxRetries, e.LastErr.Error())
}

func (e *MaxRetryError) Unwrap() error {
	return e.LastErr
}

func (e *MaxRetryError) GRPCStatus() *status.Status {
	s, ok := status.FromError(e.Unwrap())
	if !ok {
		return nil
	}
	return s
}

// ResettableError wraps an error that requires connection reset and retry on a different node.
type ResettableError struct {
	Err error
}

func (e *ResettableError) Error() string {
	s := errMsgResettable
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

func (e *ResettableError) Unwrap() error {
	return e.Err
}

func (e *ResettableError) GRPCStatus() *status.Status {
	return status.New(codes.Unavailable, e.Error())
}

// RetryableError wraps an error that can be retried on the same connection.
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	s := errMsgRetryable
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}
