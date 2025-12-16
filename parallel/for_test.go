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

package parallel

import (
	"context"
	"testing"

	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-tasks/internal/testcontext"
	"github.com/stretchr/testify/assert"
)

func TestParallelFor_Success(t *testing.T) {
	numIndexes := uint(50)
	numWorkers := uint(5)
	expectedVisited := make([]bool, numIndexes)
	actualVisited := make([]bool, numIndexes)
	for idx := range numIndexes {
		expectedVisited[idx] = true
	}
	err := For(testcontext.GetTestContext(t), numWorkers, numIndexes, func(ctx context.Context, idx uint) error {
		actualVisited[idx] = true
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, expectedVisited, actualVisited)
}

func TestParallelFor_Error(t *testing.T) {
	numIndexes := uint(50)
	numWorkers := uint(5)
	expectedVisited := make([]bool, numIndexes)
	actualVisited := make([]bool, numIndexes)
	for idx := range numIndexes {
		expectedVisited[idx] = true
	}
	err := For(testcontext.GetTestContext(t), numWorkers, numIndexes, func(ctx context.Context, idx uint) error {
		actualVisited[idx] = true
		return werror.Error("error message")
	})
	assert.Error(t, err)
	assert.Equal(t, expectedVisited, actualVisited)
}

func TestParallelForever_Panic(t *testing.T) {
	firstChan := make(chan int)
	secondChan := make(chan int)
	Forever(testcontext.GetTestContext(t), 2, func(ctx context.Context, idx uint) error {
		if idx == 0 {
			<-firstChan
			panic("oops!")
		}
		firstChan <- 1
		secondChan <- 2
		// never finishes
		secondChan <- 3
		return nil
	})
	assert.Equal(t, <-secondChan, 2)
	assert.Equal(t, len(firstChan), 0)
}
