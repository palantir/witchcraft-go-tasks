package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToPointer(t *testing.T) {
	testValue := "foo"
	testValuePointer := ToPointer(testValue)
	assert.Equal(t, &testValue, testValuePointer)
}

func TestToValue(t *testing.T) {
	testValue := "foo"
	testValueDeRef := ToValue(&testValue)
	assert.Equal(t, testValue, testValueDeRef)
}

type TestValue struct {
	Value string
}

func TestZero(t *testing.T) {
	assert.Equal(t, TestValue{}, Zero[TestValue]())
}
