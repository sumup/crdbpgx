package crdbpgx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMaxConnsPerNode(t *testing.T) {
	t.Parallel()

	t.Run("allows any node to hold the remainder connection", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 4, maxConnsPerNode(10, 3))
	})

	t.Run("does not add headroom when connections divide evenly", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 5, maxConnsPerNode(10, 2))
	})
}
