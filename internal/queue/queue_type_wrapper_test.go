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

package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueueWrapper_PushAndPop(t *testing.T) {
	q := new(queueWrapper[string])
	q.Push("item1")
	q.Push("item2")
	assert.Equal(t, 2, q.Len())
	item := q.Pop()
	assert.Equal(t, "item1", item)
	assert.Equal(t, 1, q.Len())
	item = q.Pop()
	assert.Equal(t, "item2", item)
	assert.Equal(t, 0, q.Len())
}

func TestQueueWrapper_FIFOOrder(t *testing.T) {
	q := new(queueWrapper[int])
	for i := range 5 {
		q.Push(i)
	}
	for i := range 5 {
		item := q.Pop()
		assert.Equal(t, i, item)
	}
}
