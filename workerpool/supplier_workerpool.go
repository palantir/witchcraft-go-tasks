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
	"context"
	"sync/atomic"

	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/palantir/witchcraft-go-tasks/internal/queue"
	"github.com/palantir/witchcraft-go-tasks/util/async"
	"github.com/palantir/witchcraft-go-tasks/workerpool/internal"
)

const (
	cacheMetricName    = "com.palantir.workerpool.workers"
	enqueuedMetricName = "com.palantir.workerpool.queued"
)

type defaultSupplierWorkerPool[R any] struct {
	config                        Config
	queue                         queue.Queue[workerPoolWrapperObject[R]]
	numberFree                    atomic.Int64
	totalCount                    atomic.Int64
	parentContextForWorkerThreads context.Context
}

type workerPoolWrapperObject[R any] struct {
	contextToRunFutureWith context.Context
	underlyingFuture       internal.ComputingFuture[R]
}

// NewDefaultSupplierWorkerPool creates a default SupplierWorkerPool
func NewDefaultSupplierWorkerPool[R any](ctx context.Context, options ...Option) SupplierWorkerPool[R] {
	config := &Config{}
	for _, option := range options {
		option(config)
	}
	d := &defaultSupplierWorkerPool[R]{
		queue:                         queue.NewQueue[workerPoolWrapperObject[R]](),
		config:                        *config,
		parentContextForWorkerThreads: ctx,
	}
	d.shutDownQueueIfNeeded()
	return d
}

func (d *defaultSupplierWorkerPool[R]) Submit(ctxFromClient context.Context, supplier function.Supplier[R]) async.Future[R] {
	if d.needAdditionalWorker() {
		d.startWorkerAsync()
		d.markWorkerCount()
	}
	computingFuture := internal.NewDefaultComputingFuture(supplier)
	workerPoolWrapperObject := workerPoolWrapperObject[R]{
		contextToRunFutureWith: ctxFromClient,
		underlyingFuture:       computingFuture,
	}
	d.queue.Add(workerPoolWrapperObject)
	d.markQueueLength()
	return computingFuture
}

func (d *defaultSupplierWorkerPool[R]) SubmitWithCallback(ctxFromClient context.Context, supplier function.Supplier[R], onComplete func(context.Context, R, error)) {
	d.Submit(ctxFromClient, function.NewSupplierFromFunc(func(ctx context.Context) (R, error) {
		result, err := supplier.Get(ctx)
		onComplete(ctx, result, err)
		return result, err
	}))
}

func (d *defaultSupplierWorkerPool[R]) startWorker(ctx context.Context) {
	runFn := func(ctx context.Context) {
		d.runWorkerLoop(ctx)
	}
	wapp.RunWithRecoveryLogging(ctx, runFn)
	// If we exit for some reason and the queue is not shutting down, retry
	if d.queue.ShuttingDown() {
		return
	}
	d.startWorker(ctx)
}

func (d *defaultSupplierWorkerPool[R]) getCurrentCount() int {
	return int(d.totalCount.Load())
}

func (d *defaultSupplierWorkerPool[R]) startWorkerAsync() {
	d.totalCount.Add(1)
	workerID := d.getCurrentCount()
	ctx := svc1log.WithLoggerParams(d.parentContextForWorkerThreads, svc1log.SafeParam("workerID", workerID))
	go d.startWorker(ctx)
}
func (d *defaultSupplierWorkerPool[R]) runWorkerLoop(workerContext context.Context) {
	// initialSubmitMade is needed so that we ensure that the first submission was tied to the submit that triggered it
	initialSubmitMade := false
	for {
		element, shutdown := d.queue.GetWithCallback(func() {
			if initialSubmitMade {
				d.numberFree.Add(-1)
			}
			initialSubmitMade = true
		})
		if shutdown {
			svc1log.FromContext(workerContext).Warn("Queue shutting down; workers stopping.")
			return
		}

		element.underlyingFuture.Compute(element.contextToRunFutureWith)
		d.markQueueLength()
		d.numberFree.Add(1)
	}
}

func (d *defaultSupplierWorkerPool[R]) needAdditionalWorker() bool {
	notDoingWorkCount := int(d.numberFree.Load())
	mapSize := d.getCurrentCount()
	if d.config.maxNumberOfWorkers != nil && *d.config.maxNumberOfWorkers == mapSize {
		return false
	}
	// If everything is working, we must give a new worker
	if notDoingWorkCount == 0 {
		return true
	}
	// Compare workers about to start vs the queue size, before and after computation
	length := d.queue.Len()
	if notDoingWorkCount <= length {
		return true
	}
	return false
}

func (d *defaultSupplierWorkerPool[R]) markWorkerCount() {
	if len(d.config.tags) > 0 {
		metrics.FromContext(d.parentContextForWorkerThreads).Gauge(cacheMetricName, d.config.tags...).Update(int64(d.getCurrentCount()))
	}
}

func (d *defaultSupplierWorkerPool[R]) markQueueLength() {
	if len(d.config.tags) > 0 {
		metrics.FromContext(d.parentContextForWorkerThreads).Gauge(enqueuedMetricName, d.config.tags...).Update(int64(d.queue.Len()))
	}
}

func (d *defaultSupplierWorkerPool[R]) shutDownQueueIfNeeded() {
	go func() {
		for {
			select {
			case <-d.parentContextForWorkerThreads.Done():
				d.queue.ShutDown()
				svc1log.FromContext(d.parentContextForWorkerThreads).Warn("Worker pool shut down, shutting down the queue")
				return
			}
		}
	}()
}
