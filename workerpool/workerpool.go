package workerpool

import (
	"context"
	"time"

	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	workqueue "github.com/palantir/witchcraft-go-tasks/internal/queue"
	"github.com/palantir/witchcraft-go-tracing/wtracing"
)

const maxNumRequeues = 5

type defaultWorkerPool[T ElementIdentifier] struct {
	elementProcessor ElementProcessor[T]
	// k8sKeyedErrorHealthCheckSource observability.K8sKeyedErrorHealthCheckSource
	numWorkers     int
	queue          workqueue.Queue
	workerPoolName string
}

// NewDefaultWorkerPool instantiates a worker pool
func NewDefaultWorkerPool[T ElementIdentifier](
	elementProcessor ElementProcessor[T],
	numWorkers int,
	queue workqueue.Queue,
// k8sKeyedErrorHealthCheckSource observability.K8sKeyedErrorHealthCheckSource,
	workerPoolName string) K8sWorkerPool[T] {
	return &defaultWorkerPool[T]{
		// k8sKeyedErrorHealthCheckSource: k8sKeyedErrorHealthCheckSource,
		elementProcessor: elementProcessor,
		queue:            queue,
		numWorkers:       numWorkers,
		workerPoolName:   workerPoolName,
	}
}

func (d defaultWorkerPool[T]) Start(ctx context.Context) {
	ctx = svc1log.WithLoggerParams(ctx, svc1log.SafeParam("workerPoolName", d.workerPoolName))
	for i := 0; i < d.numWorkers; i++ {
		d.startWorkerAsync(ctx, i)
	}
}

func (d defaultWorkerPool[T]) Submit(ctx context.Context, element T) {
	d.queue.Add(element)
}

func (d defaultWorkerPool[T]) startWorkerAsync(ctx context.Context, workerID int) {
	ctx = svc1log.WithLoggerParams(ctx, svc1log.SafeParam("workerID", workerID))
	go d.startWorker(ctx)
}

func (d defaultWorkerPool[T]) startWorker(ctx context.Context) {
	wapp.RunWithRecoveryLogging(ctx, d.runWorkerLoop)
	// If we exit for some reason and the queue is not shutting down, retry
	if d.queue.ShuttingDown() {
		return
	}
	d.startWorker(ctx)
}

func (d defaultWorkerPool[T]) runWorkerLoop(ctx context.Context) {
	for {
		element, shutdown := d.queue.Get()
		if shutdown {
			svc1log.FromContext(ctx).Warn("Queue shutting down; workers stopping.")
			return
		}
		typedElement, ok := element.(T)
		if !ok {
			svc1log.FromContext(ctx).Error("Unexpected queue element type; this should never happen!")
			d.queue.Done(element)
			d.queue.Forget(element)
			continue
		}
		d.singleProcessAttempt(ctx, typedElement)
		wccmetrics.WccMetrics(ctx).QueueLength().WorkerPoolName(d.workerPoolName).Gauge().Update(int64(d.queue.Len()))
	}
}

func (d defaultWorkerPool[T]) singleProcessAttempt(ctx context.Context, typedElement T) {
	span, ctx := wtracing.StartSpanFromTracerInContext(ctx, "defaultWorkerPool.runSingleJob")
	defer span.Finish()
	ctx = svc1log.WithLoggerParams(ctx, svc1log.SafeParam("queueElementIdentifier", typedElement.GetIdentifier()))
	if err := d.runProcessorSingleTime(ctx, typedElement); err != nil {
		d.handleProcessError(ctx, typedElement, err)
		return
	}
	d.queue.Done(typedElement)
	d.queue.Forget(typedElement)
	d.k8sKeyedErrorHealthCheckSource.Submit(ctx, typedElement.GetIdentifier(), nil)
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
	d.k8sKeyedErrorHealthCheckSource.Submit(ctx, element.GetIdentifier(), err)
	numRequeues := d.queue.NumRequeues(element)
	ctx = svc1log.WithLoggerParams(ctx, svc1log.SafeParam("maxNumRequeues", maxNumRequeues), svc1log.SafeParam("numRequeues", numRequeues))
	d.queue.Done(element)
	if numRequeues == maxNumRequeues {
		observability.LogErrorTalkingToK8s(ctx, "Forgetting queue element after multiple failed process attempts.", err)
		d.queue.Forget(element)
	} else {
		observability.LogErrorTalkingToK8s(ctx, "Failed to process queue element; requeuing.", err)
		d.queue.AddRateLimited(element)
	}
}
