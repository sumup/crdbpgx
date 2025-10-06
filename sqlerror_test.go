package crdbpgx

import (
	"errors"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMaxRetryError(t *testing.T) {
	t.Run("Error()", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			err  *MaxRetryError
			want string
		}{
			{
				name: "no retries, no error",
				err:  &MaxRetryError{},
				want: "retries disabled",
			},
			{
				name: "retries, no error",
				err:  &MaxRetryError{MaxRetries: 10},
				want: "max retries reached (10)",
			},
			{
				name: "retries, error",
				err: &MaxRetryError{
					MaxRetries: 10,
					LastErr:    errors.New("testerr"),
				},
				want: "max retries reached (10): testerr",
			},
			{
				name: "no retries, error",
				err: &MaxRetryError{
					LastErr: errors.New("testerr"),
				},
				want: "retries disabled: testerr",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if got := tc.err.Error(); got != tc.want {
					t.Errorf("Error() = %s, want %s", got, tc.want)
				}
			})
		}
	})

	t.Run("GRPCStatus()", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			err  *MaxRetryError
			want *status.Status
		}{
			{
				name: "no retries, no error",
				err:  &MaxRetryError{},
				want: nil,
			},
			{
				name: "retries, no error",
				err:  &MaxRetryError{MaxRetries: 10},
				want: nil,
			},
			{
				name: "retries, error",
				err: &MaxRetryError{
					MaxRetries: 10,
					LastErr:    errors.New("testerr"),
				},
				want: nil,
			},
			{
				name: "no retries, error",
				err: &MaxRetryError{
					LastErr: errors.New("testerr"),
				},
				want: nil,
			},
			{
				name: "retries, grpc status",
				err: &MaxRetryError{
					MaxRetries: 10,
					LastErr:    status.Error(codes.Unavailable, "testerr"),
				},
				want: status.New(codes.Unavailable, "testerr"),
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if got := tc.err.GRPCStatus(); !reflect.DeepEqual(got, tc.want) {
					t.Errorf("GRPCStatus() = %v, want %v", got, tc.want)
				}
			})
		}
	})

	t.Run("Unwrap()", func(t *testing.T) {
		testErr := errors.New("testerr")

		err := &MaxRetryError{
			LastErr: testErr,
		}

		if !errors.Is(err, testErr) {
			t.Errorf("Unwrap() = %v want %v", testErr, err)
		}

		err2 := &MaxRetryError{}
		if err2.Unwrap() != nil {
			t.Errorf("Unwrap() = %v, want nil", err2.Unwrap())
		}
	})
}

func Test_sqlErrorCode(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{
			name: "nil",
			err:  nil,
			want: "",
		},
		{
			name: "not a pg error",
			err:  errors.New("some nonpg error"),
			want: "",
		},
		{
			name: "pg error",
			err:  &pgconn.PgError{Code: "4001"},
			want: "4001",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sqlErrorCode(tc.err); got != tc.want {
				t.Errorf("sqlErrorCode() = %v, want %v", got, tc.want)
			}
		})
	}
}
