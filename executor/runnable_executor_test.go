package executor

import (
	"context"
	"testing"

	werror "github.com/palantir/witchcraft-go-error"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	workerpool_mock "github.palantir.build/deployability/generics-pkg/internal/generated/mocks/github.palantir.build/deployability/generics-pkg/workerpool"
	internal_mock "github.palantir.build/deployability/generics-pkg/internal/generated/mocks/github.palantir.build/deployability/generics-pkg/workerpool/internal_"
	"github.palantir.build/deployability/generics-pkg/util/functional"
	"github.palantir.build/deployability/generics-pkg/workerpool"
)

func Test_ExecuteRunnables(t *testing.T) {
	f1 := new(internal_mock.ComputingFuture[struct{}])
	f2 := new(internal_mock.ComputingFuture[struct{}])
	voidWorkerPool := new(workerpool_mock.VoidWorkerPool)
	runnableExecutor := NewDefaultRunnableExecutor(voidWorkerPool)
	r1 := functional.NewRunnableFromFunc(func(ctx context.Context) error {
		return nil
	})
	r2 := functional.NewRunnableFromFunc(func(ctx context.Context) error {
		return nil
	})
	voidWorkerPool.On("Submit", mock.Anything, mock.AnythingOfType("functional.RunnableFunc")).Return(f1).Times(1)
	voidWorkerPool.On("Submit", mock.Anything, mock.AnythingOfType("functional.RunnableFunc")).Return(f2).Times(1)
	f1.On("Get", mock.Anything).Return(struct{}{}, nil)
	f2.On("Get", mock.Anything).Return(struct{}{}, nil)
	errs := runnableExecutor.ExecuteRunnables(context.Background(), []functional.Runnable{r1, r2})
	assert.Empty(t, errs)
}

func Test_ExecuteRunnables_WithError(t *testing.T) {
	f1 := new(internal_mock.ComputingFuture[struct{}])
	f2 := new(internal_mock.ComputingFuture[struct{}])
	voidWorkerPool := new(workerpool_mock.VoidWorkerPool)
	runnableExecutor := NewDefaultRunnableExecutor(voidWorkerPool)
	r1 := functional.NewRunnableFromFunc(func(ctx context.Context) error {
		return nil
	})
	r2 := functional.NewRunnableFromFunc(func(ctx context.Context) error {
		return nil
	})
	voidWorkerPool.On("Submit", mock.Anything, mock.AnythingOfType("functional.RunnableFunc")).Return(f1).Times(1)
	voidWorkerPool.On("Submit", mock.Anything, mock.AnythingOfType("functional.RunnableFunc")).Return(f2).Times(1)
	f1.On("Get", mock.Anything).Return(struct{}{}, nil)
	f2.On("Get", mock.Anything).Return(struct{}{}, werror.Error("err here"))
	errs := runnableExecutor.ExecuteRunnables(context.Background(), []functional.Runnable{r1, r2})
	assert.Equal(t, 1, len(errs))
	assert.EqualError(t, errs[0], "err here")
}

func Test_ExecuteRunnablesNoMocks(t *testing.T) {
	voidWorkerPool := workerpool.NewDefaultVoidWorkerPool(context.Background())
	runnableExecutor := NewDefaultRunnableExecutor(voidWorkerPool)
	r1Called := false
	r1 := functional.NewRunnableFromFunc(func(ctx context.Context) error {
		r1Called = true
		return nil
	})
	r2Called := false
	r2 := functional.NewRunnableFromFunc(func(ctx context.Context) error {
		r2Called = true
		return nil
	})
	errs := runnableExecutor.ExecuteRunnables(context.Background(), []functional.Runnable{r1, r2})
	assert.Empty(t, errs)
	assert.True(t, r1Called)
	assert.True(t, r2Called)
}

func Test_ExecuteRunnable(t *testing.T) {
	f1 := new(internal_mock.ComputingFuture[struct{}])
	voidWorkerPool := new(workerpool_mock.VoidWorkerPool)
	runnableExecutor := NewDefaultRunnableExecutor(voidWorkerPool)
	r1 := functional.NewRunnableFromFunc(func(ctx context.Context) error {
		return nil
	})
	voidWorkerPool.On("Submit", mock.Anything, mock.AnythingOfType("functional.RunnableFunc")).Return(f1).Times(1)
	f1.On("Get", mock.Anything).Return(struct{}{}, nil)
	err := runnableExecutor.ExecuteRunnable(context.Background(), r1)
	assert.NoError(t, err)
}

func Test_ExecuteRunnableErrors(t *testing.T) {
	f2 := new(internal_mock.ComputingFuture[struct{}])
	voidWorkerPool := new(workerpool_mock.VoidWorkerPool)
	runnableExecutor := NewDefaultRunnableExecutor(voidWorkerPool)
	r2 := functional.NewRunnableFromFunc(func(ctx context.Context) error {
		return nil
	})
	voidWorkerPool.On("Submit", mock.Anything, mock.AnythingOfType("functional.RunnableFunc")).Return(f2).Times(1)
	f2.On("Get", mock.Anything).Return(struct{}{}, werror.Error("err here"))
	err := runnableExecutor.ExecuteRunnable(context.Background(), r2)
	assert.EqualError(t, err, "err here")
}
