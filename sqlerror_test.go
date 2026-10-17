package crdbpgx

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMaxRetryError(t *testing.T) {
	t.Parallel()

	t.Run("Error()", func(t *testing.T) {
		t.Parallel()

		t.Run("no retries, no error", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, "retries disabled", (&MaxRetryError{}).Error())
		})
		t.Run("retries, no error", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, "max retries reached (10)", (&MaxRetryError{MaxRetries: 10}).Error())
		})
		t.Run("retries, error", func(t *testing.T) {
			t.Parallel()
			err := &MaxRetryError{MaxRetries: 10, LastErr: errors.New("testerr")}
			assert.Equal(t, "max retries reached (10): testerr", err.Error())
		})
		t.Run("no retries, error", func(t *testing.T) {
			t.Parallel()
			err := &MaxRetryError{LastErr: errors.New("testerr")}
			assert.Equal(t, "retries disabled: testerr", err.Error())
		})
	})

	t.Run("GRPCStatus()", func(t *testing.T) {
		t.Parallel()

		t.Run("no retries, no error", func(t *testing.T) {
			t.Parallel()
			assert.Nil(t, (&MaxRetryError{}).GRPCStatus())
		})
		t.Run("retries, no error", func(t *testing.T) {
			t.Parallel()
			assert.Nil(t, (&MaxRetryError{MaxRetries: 10}).GRPCStatus())
		})
		t.Run("retries, ordinary error", func(t *testing.T) {
			t.Parallel()
			err := &MaxRetryError{MaxRetries: 10, LastErr: assert.AnError}
			assert.Nil(t, err.GRPCStatus())
		})
		t.Run("no retries, ordinary error", func(t *testing.T) {
			t.Parallel()
			err := &MaxRetryError{LastErr: assert.AnError}
			assert.Nil(t, err.GRPCStatus())
		})
		t.Run("retries, grpc status", func(t *testing.T) {
			t.Parallel()
			err := &MaxRetryError{MaxRetries: 10, LastErr: status.Error(codes.Unavailable, "testerr")}
			assert.Equal(t, status.New(codes.Unavailable, "testerr"), err.GRPCStatus())
		})
	})

	t.Run("Unwrap()", func(t *testing.T) {
		testErr := errors.New("testerr")

		err := &MaxRetryError{
			LastErr: testErr,
		}

		assert.ErrorIs(t, err, testErr)

		err2 := &MaxRetryError{}
		assert.Nil(t, err2.Unwrap())
	})
}

func Test_sqlErrorCode(t *testing.T) {
	t.Parallel()

	t.Run("nil", func(t *testing.T) {
		t.Parallel()

		assert.Empty(t, sqlErrorCode(nil))
	})

	t.Run("not a pg error", func(t *testing.T) {
		t.Parallel()

		assert.Empty(t, sqlErrorCode(assert.AnError))
	})

	t.Run("pg error", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "4001", sqlErrorCode(&pgconn.PgError{Code: "4001"}))
	})
}

func TestSQLErrorClassification(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		err        error
		retryable  bool
		resettable bool
	}{
		"ordinary error":        {err: assert.AnError},
		"serialization failure": {err: &pgconn.PgError{Code: CrdbRetryErrCode}, retryable: true},
		"ambiguous result":      {err: &pgconn.PgError{Code: CrdbAmbiguousErrorCode}, resettable: true},
		"node shutdown":         {err: &pgconn.PgError{Code: CrdbServerNotAcceptingClients}, resettable: true},
		"clock skew":            {err: &pgconn.PgError{Code: CrdbUnknownSQLState, Message: CrdbClockSkewMessage}, retryable: true},
		"other internal error":  {err: &pgconn.PgError{Code: CrdbUnknownSQLState}},
		"constraint violation":  {err: &pgconn.PgError{Code: "23505"}},
		"broken pipe":           {err: errors.New("write: broken pipe"), resettable: true},
		"unexpected EOF":        {err: errors.New("read: unexpected EOF"), resettable: true},
		"closed connection":     {err: errors.New("conn closed"), resettable: true},
		"connection refused":    {err: errors.New("dial: connection refused"), resettable: true},
		"connection reset":      {err: errors.New("read: connection reset by peer"), resettable: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			for _, wrapped := range []bool{false, true} {
				name := "direct"
				err := tc.err
				if wrapped {
					name = "wrapped"
					err = fmt.Errorf("query: %w", err)
				}
				t.Run(name, func(t *testing.T) {
					assert.Equal(t, tc.retryable, IsRetryableError(t.Context(), err))
					assert.Equal(t, tc.resettable, IsResettableError(t.Context(), err))

					got := wrapSQLError(t.Context(), err)
					assert.ErrorIs(t, got, tc.err)
					switch {
					case tc.resettable:
						require.IsType(t, &ResettableError{}, got)
					case tc.retryable:
						require.IsType(t, &RetryableError{}, got)
					default:
						assert.Same(t, err, got)
					}
				})
			}
		})
	}
}

func TestWrapSQLError(t *testing.T) {
	t.Parallel()

	t.Run("nil", func(t *testing.T) {
		t.Parallel()
		assert.NoError(t, wrapSQLError(t.Context(), nil))
	})

	t.Run("context error takes precedence", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		assert.ErrorIs(t, wrapSQLError(ctx, &pgconn.PgError{Code: CrdbRetryErrCode}), context.Canceled)
		assert.NoError(t, wrapSQLError(ctx, nil), "successful operations remain successful")

		expiredCtx, cancelExpired := context.WithDeadline(t.Context(), time.Time{})
		defer cancelExpired()
		assert.ErrorIs(t, wrapSQLError(expiredCtx, &pgconn.PgError{Code: CrdbRetryErrCode}), context.DeadlineExceeded)
	})

	t.Run("preserves operation cancellation", func(t *testing.T) {
		t.Parallel()

		for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
			err := fmt.Errorf("query: %w", cause)
			assert.Same(t, err, wrapSQLError(t.Context(), err))
		}
	})
}
