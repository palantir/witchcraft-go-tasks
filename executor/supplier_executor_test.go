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
)

func Test_SupplierExecutorResolveAll(t *testing.T) {
	f1 := new(internal_mock.ComputingFuture[string])
	f2 := new(internal_mock.ComputingFuture[string])
	voidWorkerPool := new(workerpool_mock.WorkerPool[string])
	runnableExecutor := NewDefaultSupplierExecutor[string](voidWorkerPool)
	r1 := functional.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "1", nil
	})
	r2 := functional.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "2", nil
	})
	voidWorkerPool.On("Submit", mock.Anything, mock.AnythingOfType("functional.SupplierFunc[string]")).Return(f1).Times(1)
	voidWorkerPool.On("Submit", mock.Anything, mock.AnythingOfType("functional.SupplierFunc[string]")).Return(f2).Times(1)
	f1.On("Get", mock.Anything).Return("1", nil)
	f2.On("Get", mock.Anything).Return("2", nil)
	result, errs := runnableExecutor.ResolveAll(context.Background(), []functional.Supplier[string]{r1, r2})
	assert.Empty(t, errs)
	assert.Equal(t, []string{"1", "2"}, result)
}

func Test_SupplierExecutor_ResolveUntilError(t *testing.T) {
	f1 := new(internal_mock.ComputingFuture[string])
	f2 := new(internal_mock.ComputingFuture[string])
	voidWorkerPool := new(workerpool_mock.WorkerPool[string])
	runnableExecutor := NewDefaultSupplierExecutor[string](voidWorkerPool)
	r1 := functional.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "1", nil
	})
	r2 := functional.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "2", nil
	})
	voidWorkerPool.On("Submit", mock.Anything, mock.AnythingOfType("functional.SupplierFunc[string]")).Return(f1).Times(1)
	voidWorkerPool.On("Submit", mock.Anything, mock.AnythingOfType("functional.SupplierFunc[string]")).Return(f2).Times(1)
	f1.On("Get", mock.Anything).Return("1", nil)
	f2.On("Get", mock.Anything).Return("2", nil)
	result, err := runnableExecutor.ResolveUntilError(context.Background(), []functional.Supplier[string]{r1, r2})
	assert.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, result)
}

func Test_SupplierExecutorResolveAllSomeErrors(t *testing.T) {
	f1 := new(internal_mock.ComputingFuture[string])
	f2 := new(internal_mock.ComputingFuture[string])
	f3 := new(internal_mock.ComputingFuture[string])
	f4 := new(internal_mock.ComputingFuture[string])
	voidWorkerPool := new(workerpool_mock.WorkerPool[string])
	runnableExecutor := NewDefaultSupplierExecutor[string](voidWorkerPool)
	r1 := functional.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "1", nil
	})
	r2 := functional.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "2", nil
	})
	r3 := functional.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "3", nil
	})
	r4 := functional.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "4", nil
	})
	voidWorkerPool.On("Submit", mock.Anything, mock.AnythingOfType("functional.SupplierFunc[string]")).Return(f1).Times(1)
	voidWorkerPool.On("Submit", mock.Anything, mock.AnythingOfType("functional.SupplierFunc[string]")).Return(f2).Times(1)
	voidWorkerPool.On("Submit", mock.Anything, mock.AnythingOfType("functional.SupplierFunc[string]")).Return(f3).Times(1)
	voidWorkerPool.On("Submit", mock.Anything, mock.AnythingOfType("functional.SupplierFunc[string]")).Return(f4).Times(1)
	f1.On("Get", mock.Anything).Return("1", nil)
	f2.On("Get", mock.Anything).Return("", werror.Error("err"))
	f3.On("Get", mock.Anything).Return("3", nil)
	f4.On("Get", mock.Anything).Return("4", werror.Error("err"))
	result, errs := runnableExecutor.ResolveAll(context.Background(), []functional.Supplier[string]{r1, r2, r3, r4})
	assert.Equal(t, 2, len(errs))
	assert.Equal(t, []string{"1", "3"}, result)
}

func Test_SupplierExecutorResolveUntilErrorSomeErrors(t *testing.T) {
	f1 := new(internal_mock.ComputingFuture[string])
	f2 := new(internal_mock.ComputingFuture[string])
	f3 := new(internal_mock.ComputingFuture[string])
	f4 := new(internal_mock.ComputingFuture[string])
	voidWorkerPool := new(workerpool_mock.WorkerPool[string])
	runnableExecutor := NewDefaultSupplierExecutor[string](voidWorkerPool)
	r1 := functional.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "1", nil
	})
	r2 := functional.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "2", nil
	})
	r3 := functional.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "3", nil
	})
	r4 := functional.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "4", nil
	})
	voidWorkerPool.On("Submit", mock.Anything, mock.AnythingOfType("functional.SupplierFunc[string]")).Return(f1).Times(1)
	voidWorkerPool.On("Submit", mock.Anything, mock.AnythingOfType("functional.SupplierFunc[string]")).Return(f2).Times(1)
	voidWorkerPool.On("Submit", mock.Anything, mock.AnythingOfType("functional.SupplierFunc[string]")).Return(f3).Times(1)
	voidWorkerPool.On("Submit", mock.Anything, mock.AnythingOfType("functional.SupplierFunc[string]")).Return(f4).Times(1)
	f1.On("Get", mock.Anything).Return("1", nil)
	f2.On("Get", mock.Anything).Return("", werror.Error("err"))
	f3.On("Get", mock.Anything).Return("3", nil)
	f4.On("Get", mock.Anything).Return("4", werror.Error("err"))
	result, err := runnableExecutor.ResolveUntilError(context.Background(), []functional.Supplier[string]{r1, r2, r3, r4})
	assert.Error(t, err)
	assert.Equal(t, []string(nil), result)
}
