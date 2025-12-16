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

package runnable

import (
	"context"
	"testing"

	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/palantir/witchcraft-go-tasks/internal/testcontext"
	"github.com/stretchr/testify/assert"
)

func TestForeverRunnable(t *testing.T) {
	ctx := testcontext.GetTestContext(t)
	started1 := make(chan struct{})
	started2 := make(chan struct{})
	neverDone := make(chan struct{})

	runnable1 := New("runnable-1", func(ctx context.Context) error {
		close(started1)
		<-neverDone
		return nil
	})
	runnable2 := New("runnable-2", func(ctx context.Context) error {
		close(started2)
		panic("oops!")
	})
	runnable := NewForever("", []function.NamedRunnable{runnable1, runnable2})

	runnableExited := make(chan struct{})
	defer func() {
		<-runnableExited
	}()
	go func() {
		err := runnable.Run(ctx)
		assert.NoError(t, err)
		close(runnableExited)
	}()
	<-started1
	<-started2
}
