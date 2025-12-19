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

package executor

import (
	"context"
)

// ItemSubmitterOption is an option that can be used to configure ItemSubmitters created using the NewDefaultItemSubmitter function.
type ItemSubmitterOption[T constraint] interface {
	apply(*defaultItemSubmitter[T])
}

type itemSubmitterOptionFunc[T constraint] func(*defaultItemSubmitter[T])

func (f itemSubmitterOptionFunc[T]) apply(i *defaultItemSubmitter[T]) {
	f(i)
}

// WithMaxNumRequeues sets the maximum number of times an item will be requeued
// after processing failures before being dropped. Defaults to 5 if not specified.
func WithMaxNumRequeues[T constraint](maxNumRequeues int) ItemSubmitterOption[T] {
	return itemSubmitterOptionFunc[T](func(defaultItemSubmitterArg *defaultItemSubmitter[T]) {
		defaultItemSubmitterArg.maxNumRequeues = maxNumRequeues
	})
}

// WithErrorLogger sets a custom error logging function called when item processing
// fails. By default, errors are logged using svc1log at ERROR level with a stacktrace.
func WithErrorLogger[T constraint](errorLogger func(ctx context.Context, err error)) ItemSubmitterOption[T] {
	return itemSubmitterOptionFunc[T](func(defaultItemSubmitterArg *defaultItemSubmitter[T]) {
		defaultItemSubmitterArg.logError = errorLogger
	})
}
