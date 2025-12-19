package notsure

import (
	"context"
	"fmt"
	"time"

	"github.com/palantir/witchcraft-go-health/v2/sources/window"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	"github.com/palantir/witchcraft-go-tasks/internal/queue"
	"github.com/palantir/witchcraft-go-tasks/workerpool"
	"github.com/palantir/witchcraft-go-tracing/wtracing"
)

type constraint interface {
	comparable
	fmt.Stringer
}

// K8sWorkerPool is a wrapper around a rate limited queue backed by the K8s util library
type K8sWorkerPool[T constraint] interface {
	// Submit adds an element to the queue for eventual consumption
	Submit(context.Context, T)
}
type defaultWorkerPool[T constraint] struct {
	k8sKeyedErrorHealthCheckSource window.KeyedErrorHealthCheckSource
	queue                          queue.CollapsingQueue[T]
	consumerWorkerPool             workerpool.ConsumerWorkerPool[T]

	logError       func(ctx context.Context, err error)
	maxNumRequeues int
}

// NewDefaultWorkerPool instantiates a worker pool
func NewDefaultWorkerPool[T constraint](
	consumerWorkerPool workerpool.ConsumerWorkerPool[T],
	k8sKeyedErrorHealthCheckSource window.KeyedErrorHealthCheckSource,
	options ...JobOption[T]) K8sWorkerPool[T] {
	defaultWorkerPoolArg := &defaultWorkerPool[T]{
		k8sKeyedErrorHealthCheckSource: k8sKeyedErrorHealthCheckSource,
		queue:                          queue.NewCollapsingQueue[T](),
		logError: func(ctx context.Context, err error) {
			svc1log.FromContext(ctx).Error("error occurred processing element in workerpool", svc1log.Stacktrace(err))
		},
		maxNumRequeues: 5,
	}
	for _, option := range options {
		option.apply(defaultWorkerPoolArg)
	}
	return defaultWorkerPoolArg
}

func (d defaultWorkerPool[T]) Submit(ctx context.Context, element T) {
	d.queue.Add(element)
}

func (d defaultWorkerPool[T]) runWorkerLoopREAL(ctx context.Context) {
	for {
		element, shutdown := d.queue.Get()
		if shutdown {
			svc1log.FromContext(ctx).Warn("Queue shutting down; workers stopping.")
			return
		}
		d.singleProcessAttempt(ctx, element)
		wccmetrics.WccMetrics(ctx).QueueLength().WorkerPoolName(d.workerPoolName).Gauge().Update(int64(d.queue.Len()))

	}
}

func (d defaultWorkerPool[T]) singleProcessAttempt(ctx context.Context, element T) {
	span, ctx := wtracing.StartSpanFromTracerInContext(ctx, "defaultWorkerPool.runSingleJob")
	defer span.Finish()
	ctx = svc1log.WithLoggerParams(ctx, svc1log.SafeParam("queueElementIdentifier", element.String()))
	d.consumerWorkerPool.SubmitWithCallback(ctx, element, func(ctx context.Context, elem T, err error) {
		d.k8sKeyedErrorHealthCheckSource.Submit(ctx, elem.String(), err)
		d.queue.Done(element)
		if err != nil {
			d.handleProcessError(ctx, element, err)
			return
		}
		d.queue.Forget(element)
	})
}

func (d defaultWorkerPool[T]) runProcessorSingleTime(ctx context.Context, element T) error {
	startTime := time.Now()
	defer wccmetrics.WccMetrics(ctx).ProcessElementDuration().
		WorkerPoolName(d.workerPoolName).
		Timer().UpdateSince(startTime)
	closure := func(ctx context.Context) error {
		return d.elementProcessor.ProcessElement(ctx, element)
	}
	return wapp.RunWithRecoveryLoggingWithError(ctx, closure)
}

func (d defaultWorkerPool[T]) handleProcessError(ctx context.Context, element T, err error) {
	numRequeues := d.queue.NumRequeues(element)
	ctx = svc1log.WithLoggerParams(ctx, svc1log.SafeParam("maxNumRequeues", d.maxNumRequeues), svc1log.SafeParam("numRequeues", numRequeues))
	d.logError(ctx, err)
	if numRequeues == d.maxNumRequeues {
		d.queue.Forget(element)
	} else {
		d.queue.AddRateLimited(element)
	}
}
