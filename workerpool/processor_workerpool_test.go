package workerpool

import (
	"context"
	"strconv"
	"testing"

	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NewProcessorWorkerPool(t *testing.T) {
	ctx := context.Background()
	processorPool := NewDefaultProcessorWorkerPool(ctx, function.NewFunctionFromFunc(func(ctx context.Context, arg string) (int, error) {
		return strconv.Atoi(arg)
	}))
	threeFuture := processorPool.Submit(ctx, "3")
	threeValue, err := threeFuture.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, threeValue)

	errFuture := processorPool.Submit(ctx, "a")
	errValue, err := errFuture.Get(ctx)
	require.Error(t, err)
	require.Zero(t, errValue)
}
