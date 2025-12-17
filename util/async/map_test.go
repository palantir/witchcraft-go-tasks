package async_test

import (
	"context"
	"errors"
	"testing"

	"github.com/palantir/witchcraft-go-tasks/util/async"
	"github.com/stretchr/testify/assert"
)

func TestMapFuture_Error(t *testing.T) {
	testFuture := &testFuture[string]{
		err: errors.New("initial future error"),
	}
	mappedFuture := async.MapFuture[string, int](testFuture, func(ctx context.Context, s string) (int, error) {
		return len(s), nil
	})
	got, err := mappedFuture.Get(context.Background())
	assert.EqualError(t, err, "initial future error")
	assert.Zero(t, got)
}

func TestMapFuture_Success(t *testing.T) {
	testFuture := &testFuture[string]{
		val: "value",
	}
	mappedFuture := async.MapFuture[string, int](testFuture, func(ctx context.Context, s string) (int, error) {
		return len(s), nil
	})
	got, err := mappedFuture.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 5, got)
}

type testFuture[T any] struct {
	val T
	err error
}

func (t testFuture[T]) Get(ctx context.Context) (T, error) {
	return t.val, t.err
}
