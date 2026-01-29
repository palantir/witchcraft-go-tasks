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
type ItemSubmitterOption = func(c *ItemSubmitterConfig) *ItemSubmitterConfig

// ItemSubmitterConfig is the configuration for ItemSubmitter. Configured with given options.
type ItemSubmitterConfig struct {
	maxNumRequeues int
	logError       func(ctx context.Context, err error)
}

// WithMaxNumRequeues sets the maximum number of times an item will be requeued
// after processing failures before being dropped. Defaults to 5 if not specified.
// Any value under 1 will cause 0 requeues to occur
func WithMaxNumRequeues(maxNumRequeues int) ItemSubmitterOption {
	return func(c *ItemSubmitterConfig) *ItemSubmitterConfig {
		c.maxNumRequeues = maxNumRequeues
		return c
	}
}

// WithErrorLogger sets a custom error logging function called when item processing
// fails. By default, errors are logged using svc1log at ERROR level with a stacktrace.
func WithErrorLogger(errorLogger func(ctx context.Context, err error)) ItemSubmitterOption {
	return func(c *ItemSubmitterConfig) *ItemSubmitterConfig {
		if errorLogger == nil {
			return c
		}
		c.logError = errorLogger
		return c
	}
}
