package function

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Runnable(t *testing.T) {
	myRun := NewRunnableFromFunc(func(ctx context.Context) error {
		return nil
	})
	assert.NoError(t, myRun.Run(context.Background()))
	stringToInt := NewFunctionFromFunc(func(ctx context.Context, arg string) (int, error) {
		return 1, nil
	})
	myRun = NewRunnableFromFunction("a", stringToInt)
	assert.NoError(t, myRun.Run(context.Background()))
	myRun = NewRunnableFromSupplier(NewSupplierFromFunc(func(ctx context.Context) (int, error) {
		return 1, nil
	}))
	assert.NoError(t, myRun.Run(context.Background()))
}
