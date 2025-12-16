package async

import (
	"context"
	"testing"

	werror "github.com/palantir/witchcraft-go-error"
	"github.com/stretchr/testify/assert"
)

func Test_FunctionalFuture(t *testing.T) {
	noErrorResult := NewFunctionalFuture(func(ctx context.Context) (int, error) {
		return 1, nil
	})
	errorResult := NewFunctionalFuture(func(ctx context.Context) (int, error) {
		return 0, werror.Error("err")
	})
	r, err := noErrorResult.Get(context.Background())
	assert.Equal(t, r, 1)
	assert.NoError(t, err)
	r, err = errorResult.Get(context.Background())
	assert.Equal(t, r, 0)
	assert.Error(t, err)
}
