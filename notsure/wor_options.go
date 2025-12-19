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

package notsure

import (
	"context"
)

// JobOption is an option that can be used to configure Jobs created using the NewDefaultJob function.
type JobOption[T constraint] interface {
	apply(*defaultWorkerPool[T])
}

type jobOptionFunc[T constraint] func(*defaultWorkerPool[T])

func (f jobOptionFunc[T]) apply(job *defaultWorkerPool[T]) {
	f(job)
}

// WithInterval sets the duration between job executions.
// The interval is measured from the start of one execution to the start of the next.
// Default is 1 minute if not specified.
func WithInterval[T constraint](maxNumRequeues int) JobOption[T] {
	return jobOptionFunc[T](func(job *defaultWorkerPool[T]) {
		job.maxNumRequeues = maxNumRequeues
	})
}

// WithErrorLogger sets a custom error handler that is called when the job returns an error.
// The provided function receives the context and the error returned by the job.
// Default behavior logs the error with a stacktrace via svc1log.
func WithErrorLogger[T constraint](errorLogger func(ctx context.Context, err error)) JobOption[T] {
	return jobOptionFunc[T](func(job *defaultWorkerPool[T]) {
		job.logError = errorLogger
	})
}
