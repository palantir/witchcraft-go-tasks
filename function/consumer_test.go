// Copyright (c) 2025 Palantir Technologies. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
