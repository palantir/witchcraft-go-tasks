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
	"math"
	"sync"
	"time"
)

// RateLimiter will keep track and calculate if items should be rate limited on submission
type RateLimiter[T comparable] interface {
	// When gets an item and gets to decide how long that item should wait
	// Should also keep track on the amount of times an item has been called without ResetRateLimit being invoked
	When(item T) time.Duration
	// ResetRateLimit indicates that an item is finished being retried.  Doesn't matter whether it's for failing
	// or for success, we'll stop tracking it
	ResetRateLimit(item T)
	// NumRequeues returns back how many submissions the item has had without ResetRateLimit being called
	NumRequeues(item T) int
}

func NewItemExponentialFailureRateLimiter[T comparable](baseDelay time.Duration, maxDelay time.Duration) RateLimiter[T] {
	return &ItemExponentialFailureRateLimiter[T]{
		failures:  map[T]int{},
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
	}
}

// ItemExponentialFailureRateLimiter does a simple baseDelay*2^<num-failures> limit
// dealing with max failures and expiration are up to the caller
type ItemExponentialFailureRateLimiter[T comparable] struct {
	failuresLock sync.Mutex
	failures     map[T]int

	baseDelay time.Duration
	maxDelay  time.Duration
}

func (r *ItemExponentialFailureRateLimiter[T]) When(item T) time.Duration {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()
	exp := r.failures[item]
	r.failures[item] = r.failures[item] + 1
	// The backoff is capped such that 'calculated' value never overflows.
	backoff := float64(r.baseDelay.Nanoseconds()) * math.Pow(2, float64(exp))
	if backoff > math.MaxInt64 {
		return r.maxDelay
	}
	calculated := time.Duration(backoff)
	if calculated > r.maxDelay {
		return r.maxDelay
	}
	return calculated
}

func (r *ItemExponentialFailureRateLimiter[T]) NumRequeues(item T) int {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()
	return r.failures[item]
}

func (r *ItemExponentialFailureRateLimiter[T]) ResetRateLimit(item T) {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()
	delete(r.failures, item)
}
