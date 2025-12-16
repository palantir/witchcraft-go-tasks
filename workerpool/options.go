package workerpool

import (
	"github.com/palantir/pkg/metrics"
)

// Option is a function to modify the cache config
type Option = func(c *Config) *Config

// WithMaxNumberOfThreads allows users to set maximum number of workers the workerpool will spin up
// By default this is un-set and an unlimited number of workers may be used
// Each worker is a single go-routine
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
