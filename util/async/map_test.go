package async_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/palantir/witchcraft-go-tasks/util/async"
	"github.com/stretchr/testify/assert"
)

func TestMapFuture_Error(t *testing.T) {
	future := new(async_mock.Future[string])
	future.
		On("Get", mock.Anything).
		Return("", fmt.Errorf("initial future error")).
		Times(1)
	mappedFuture := async.MapFuture[string, int](future, func(ctx context.Context, s string) (int, error) {
		return len(s), nil
	})
	got, err := mappedFuture.Get(context.Background())
	assert.EqualError(t, err, "initial future error")
	assert.Zero(t, got)
	future.AssertExpectations(t)
}

func TestMapFuture_Success(t *testing.T) {
	future := new(async_mock.Future[string])
	future.
		On("Get", mock.Anything).
		Return("12345", nil).
		Times(1)
	mappedFuture := async.MapFuture[string, int](future, func(ctx context.Context, s string) (int, error) {
		return len(s), nil
	})
	got, err := mappedFuture.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 5, got)
	future.AssertExpectations(t)
}
