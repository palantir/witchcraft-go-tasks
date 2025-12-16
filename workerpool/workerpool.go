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

type defaultWorkerPool[T any] struct {
	config                        Config
	queue                         queue.Queue[workerPoolWrapperObject[T]]
	numberFree                    atomic.Int64
	totalCount                    atomic.Int64
	parentContextForWorkerThreads context.Context
}

type workerPoolWrapperObject[T any] struct {
	contextToRunFutureWith context.Context
	underlyingFuture       internal.ComputingFuture[T]
}

// NewDefaultWorkerPool instantiates a worker pool
// This worker pool will start with 0 workers and go-routines running
// It will increase the worker count during job submission iff all workers are working and we are below the maxNumberOfWorkers if set
// It is the worker pool that is used by the defaultVoidWorkerPool
// The context that is passed in should have all the given loggers needed
// If this context ever returns under ctx.Done(), then the queue is shut down and all work is stopped
func NewDefaultWorkerPool[T any](ctx context.Context, options ...Option) WorkerPool[T] {
	config := &Config{}
	for _, option := range options {
		option(config)
	}
	d := &defaultWorkerPool[T]{
		queue:                         queue.NewQueue[workerPoolWrapperObject[T]](),
		config:                        *config,
		parentContextForWorkerThreads: ctx,
	}
	d.shutDownQueueIfNeeded()
	return d
}

func (d *defaultWorkerPool[T]) Submit(ctxFromClient context.Context, supplier function.Supplier[T]) async.Future[T] {
	if d.needAdditionalWorker() {
		d.startWorkerAsync()
		d.markWorkerCount()
	}
	computingFuture := internal.NewDefaultComputingFuture(supplier)
	workerPoolWrapperObject := workerPoolWrapperObject[T]{
		contextToRunFutureWith: ctxFromClient,
		underlyingFuture:       computingFuture,
	}
	d.queue.Add(workerPoolWrapperObject)
	d.markQueueLength()
	return computingFuture
}

func (d *defaultWorkerPool[T]) startWorker(ctx context.Context) {
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

func (d *defaultWorkerPool[T]) getCurrentCount() int {
	return int(d.totalCount.Load())
}

func (d *defaultWorkerPool[T]) startWorkerAsync() {
	d.totalCount.Add(1)
	workerID := d.getCurrentCount()
	ctx := svc1log.WithLoggerParams(d.parentContextForWorkerThreads, svc1log.SafeParam("workerID", workerID))
	go d.startWorker(ctx)
}
func (d *defaultWorkerPool[T]) runWorkerLoop(workerContext context.Context) {
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

func (d *defaultWorkerPool[T]) needAdditionalWorker() bool {
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

func (d *defaultWorkerPool[T]) markWorkerCount() {
	if len(d.config.tags) > 0 {
		metrics.FromContext(d.parentContextForWorkerThreads).Gauge(cacheMetricName, d.config.tags...).Update(int64(d.getCurrentCount()))
	}
}

func (d *defaultWorkerPool[T]) markQueueLength() {
	if len(d.config.tags) > 0 {
		metrics.FromContext(d.parentContextForWorkerThreads).Gauge(enqueuedMetricName, d.config.tags...).Update(int64(d.queue.Len()))
	}
}

func (d *defaultWorkerPool[T]) shutDownQueueIfNeeded() {
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
