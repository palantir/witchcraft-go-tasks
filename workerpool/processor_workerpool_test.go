package workerpool

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.palantir.build/deployability/generics-pkg/util/functional"
)

func Test_NewProcessorWorkerPool(t *testing.T) {
	ctx := context.Background()
	processorPool := NewDefaultProcessorWorkerPool(ctx, functional.NewFunctionFromFunc(func(ctx context.Context, arg string) (int, error) {
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

func Test_DefaultWorkerPool2_ParallelProcessing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var value atomic.Int32
	processor := functional.NewConsumerFromFunc(func(ctx context.Context, element string) error {
		if element == "E1" {
			time.Sleep(1 * time.Second)
		}
		if element == "E1" {
			value.Store(1)
		} else if element == "E2" {
			value.Store(2)
		}
		return nil
	})
	consumerPool := NewC(ctx, processor, WithMaxNumberOfThreads(10))
	pool := CreateDefaultWorkerPool2(ctx, consumerPool)
	pool.Submit(ctx, "E1")
	pool.Submit(ctx, "E2")
	assert.Eventually(t, func() bool {
		return int32(2) == value.Load()
	}, 5*time.Second, 10*time.Millisecond)
	assert.Eventually(t, func() bool {
		return int32(1) == value.Load()
	}, 5*time.Second, 10*time.Millisecond)
}

func Test_DefaultWorkerPool2_Deduplication(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var processedCount atomic.Int32
	started := make(chan struct{})
	proceed := make(chan struct{})
	var once atomic.Bool
	processor := functional.NewConsumerFromFunc(func(ctx context.Context, element string) error {
		if once.CompareAndSwap(false, true) {
			close(started)
			<-proceed
		}
		processedCount.Add(1)
		return nil
	})
	consumerPool := NewC(ctx, processor, WithMaxNumberOfThreads(10))
	pool := CreateDefaultWorkerPool2(ctx, consumerPool)
	pool.Submit(ctx, "E1")
	pool.Submit(ctx, "E2")
	pool.Submit(ctx, "E3")
	<-started
	pool.Submit(ctx, "E1")
	pool.Submit(ctx, "E2")
	pool.Submit(ctx, "E3")
	pool.Submit(ctx, "E1")
	pool.Submit(ctx, "E2")
	pool.Submit(ctx, "E3")
	close(proceed)
	assert.Eventually(t, func() bool {
		return processedCount.Load() >= 3
	}, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, int32(3), processedCount.Load(), "each element should only be processed once due to deduplication")
}
