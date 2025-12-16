package function

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Supplier(t *testing.T) {
	stringToInt := NewFunctionFromFunc(func(ctx context.Context, arg string) (int, error) {
		return 1, nil
	})
	supplier := NewSupplierFromFunction("a", stringToInt)
	value, err := supplier.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, value)
}

func Test_SupplierFromValue(t *testing.T) {
	supplier := NewSupplierFromValue(2)
	value, err := supplier.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 2, value)
}
