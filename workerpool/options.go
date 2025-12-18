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

package workerpool

import (
	"github.com/palantir/pkg/metrics"
)

// Option is a function to modify the cache config
type Option = func(c *Config) *Config

// WithMaxNumberOfThreads allows users to set maximum number of workers the workerpool will spin up
// By default this is un-set and an unlimited number of workers may be used
// Each worker is a single goroutine
func WithMaxNumberOfThreads(maxNumberOfThreads int) Option {
	return func(c *Config) *Config {
		c.maxNumberOfWorkers = &maxNumberOfThreads
		return c
	}
}

// WithMetricTags configures the worker pool metrics on number of threads
// Metrics will only be emitted for the pool if at least 1 tag is supplied
func WithMetricTags(tags []metrics.Tag) Option {
	return func(c *Config) *Config {
		c.tags = tags
		return c
	}
}
