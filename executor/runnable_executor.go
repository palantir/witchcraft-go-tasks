// Copyright (c) 2026 Palantir Technologies. All rights reserved.
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

package executor

import (
	"context"

	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/palantir/witchcraft-go-tasks/util/async"
	"github.com/palantir/witchcraft-go-tasks/workerpool"
)

// RunnableExecutor is an interface that allows a user to submit a slice of runnables
// This parallelism of the executor is controlled by the workerpool.VoidWorkerPool given at construction time
// All runnable will be ran before returning, a list of errors will be returned if any errors are non-nil
type RunnableExecutor interface {
	ExecuteRunnables(ctx context.Context, runnables []function.Runnable) []error
	ExecuteRunnable(ctx context.Context, runnable function.Runnable) error
}

type defaultRunnableExecutor struct {
	voidWorkerPool workerpool.RunnableWorkerPool
}

// NewDefaultRunnableExecutor returns the default implementation of the RunnableExecutor
func NewDefaultRunnableExecutor(voidWorkerPool workerpool.RunnableWorkerPool) RunnableExecutor {
	return &defaultRunnableExecutor{
		voidWorkerPool: voidWorkerPool,
	}
}

func (d *defaultRunnableExecutor) ExecuteRunnables(ctx context.Context, runnables []function.Runnable) []error {
	var futures []async.VoidFuture
	for _, runnable := range runnables {
		futures = append(futures, d.voidWorkerPool.Submit(ctx, runnable))
	}
	var errList []error
	for _, future := range futures {
		_, err := future.Get(ctx)
		if err != nil {
			errList = append(errList, err)
		}
	}
	return errList
}

func (d *defaultRunnableExecutor) ExecuteRunnable(ctx context.Context, runnable function.Runnable) error {
	errList := d.ExecuteRunnables(ctx, []function.Runnable{runnable})
	if len(errList) == 0 {
		return nil
	}
	return errList[0]
}
