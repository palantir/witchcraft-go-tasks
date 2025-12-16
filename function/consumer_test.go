package function

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Consumer(t *testing.T) {
	stringToInt := NewFunctionFromFunc(func(ctx context.Context, arg string) (int, error) {
		return 1, nil
	})
	consumer := NewConsumerFromFunction(stringToInt)
	err := consumer.Accept(context.Background(), "a")
	assert.NoError(t, err)
}

func Test_ConsumerFromFunc(t *testing.T) {
	var called bool
	consumer := NewConsumerFromFunc(func(ctx context.Context, arg string) error {
		called = true
		return nil
	})
	err := consumer.Accept(context.Background(), "test")
	assert.NoError(t, err)
	assert.True(t, called)
}
