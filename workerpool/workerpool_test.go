package workerpool_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	observability_mock "github.palantir.build/deployability/witchcraft-controller-commons/internal/generated/mocks/github.palantir.build/deployability/witchcraft-controller-commons/observability"
	workerpool_mock "github.palantir.build/deployability/witchcraft-controller-commons/internal/generated/mocks/github.palantir.build/deployability/witchcraft-controller-commons/workerpool"
	"github.palantir.build/deployability/witchcraft-controller-commons/pkg/testutil"
	"github.palantir.build/deployability/witchcraft-controller-commons/workerpool"
	"k8s.io/client-go/util/workqueue"
)

const maxNumRequeues = 5

var (
	service1Elem   = testElement{WitchcraftServiceName: "service1"}
	service2Elem   = testElement{WitchcraftServiceName: "service2Elem"}
	offsetDuration = time.Millisecond * 50
	baseDuration   = time.Millisecond * 100
)

// Element in queue
type testElement struct {
	WitchcraftServiceName      string
	WitchcraftServiceNamespace string
}

func (t testElement) GetIdentifier() string {
	return t.WitchcraftServiceName
}

// Witchcraft worker pool can process a single element
func TestWitchcraftWorkerpoolConsumeSingleElement(t *testing.T) {
	ctx := testutil.GetTestContext()
	processor := new(workerpool_mock.ElementProcessor[testElement])
	processor.On("ProcessElement", mock.Anything, service1Elem).Return(nil).Times(1)
	keyedErrorSubmitter := new(observability_mock.K8sKeyedErrorHealthCheckSource)
	keyedErrorSubmitter.On("Submit", mock.Anything, service1Elem.WitchcraftServiceName, nil).Times(1)
	defer mock.AssertExpectationsForObjects(t, processor, keyedErrorSubmitter)

	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	workers := workerpool.NewDefaultWorkerPool[testElement](processor, 5, queue, keyedErrorSubmitter, "testPool")
	workers.Start(ctx)
	workers.Submit(ctx, service1Elem)
	time.Sleep(offsetDuration)
	queue.ShutDown()
}

// Witchcraft worker pool can process multiple elements
func TestWitchcraftWorkerpoolConsumeDifferentElements(t *testing.T) {
	ctx := testutil.GetTestContext()

	keyedErrorSubmitter := new(observability_mock.K8sKeyedErrorHealthCheckSource)
	keyedErrorSubmitter.On("Submit", mock.Anything, service1Elem.WitchcraftServiceName, nil).Times(1)
	keyedErrorSubmitter.On("Submit", mock.Anything, service2Elem.WitchcraftServiceName, nil).Times(1)
	processor := new(workerpool_mock.ElementProcessor[testElement])
	processor.On("ProcessElement", mock.Anything, service1Elem).Return(nil).Times(1)
	processor.On("ProcessElement", mock.Anything, service2Elem).Return(nil).Times(1)
	defer mock.AssertExpectationsForObjects(t, keyedErrorSubmitter, processor)

	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	workers := workerpool.NewDefaultWorkerPool[testElement](processor, 5, queue, keyedErrorSubmitter, "testPool")

	workers.Start(ctx)
	workers.Submit(ctx, service1Elem)
	workers.Submit(ctx, service2Elem)
	time.Sleep(offsetDuration)
	queue.ShutDown()
}

// Witchcraft worker pool won't attempt to process the same element in parallel
func TestWitchcraftWorkerpoolConsumeSameElements(t *testing.T) {
	keyedErrorSubmitter := new(observability_mock.K8sKeyedErrorHealthCheckSource)
	keyedErrorSubmitter.On("Submit", mock.Anything, service1Elem.WitchcraftServiceName, nil).Times(1)
	processor := new(workerpool_mock.ElementProcessor[testElement])
	processor.On("ProcessElement", mock.Anything, service1Elem).Return(nil).Times(1)
	defer mock.AssertExpectationsForObjects(t, keyedErrorSubmitter, processor)

	ctx := testutil.GetTestContext()
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	workers := workerpool.NewDefaultWorkerPool[testElement](processor, 5, queue, keyedErrorSubmitter, "testPool")

	workers.Start(ctx)
	workers.Submit(ctx, service1Elem)
	workers.Submit(ctx, service1Elem)
	time.Sleep(offsetDuration)
	queue.ShutDown()
}

// Witchcraft worker pool will requeue elements upon processing failure
func TestWitchcraftWorkerpoolRetryOnJobCreationError(t *testing.T) {
	err := errors.New("bad")
	keyedErrorSubmitter := new(observability_mock.K8sKeyedErrorHealthCheckSource)
	keyedErrorSubmitter.On("Submit", mock.Anything, service1Elem.WitchcraftServiceName, nil).Times(1)
	keyedErrorSubmitter.On("Submit", mock.Anything, service1Elem.WitchcraftServiceName, err).Times(1)
	processor := new(workerpool_mock.ElementProcessor[testElement])
	processor.On("ProcessElement", mock.Anything, service1Elem).Return(err).Times(1)
	processor.On("ProcessElement", mock.Anything, service1Elem).Return(nil).Times(1)
	defer mock.AssertExpectationsForObjects(t, keyedErrorSubmitter, processor)

	ctx := testutil.GetTestContext()
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	workers := workerpool.NewDefaultWorkerPool[testElement](processor, 5, queue, keyedErrorSubmitter, "testPool")

	workers.Start(ctx)
	workers.Submit(ctx, service1Elem)
	time.Sleep(baseDuration)
	queue.ShutDown()
}

// Witchcraft workerpool will give up requeuing after multiple failures
func TestWitchcraftWorkerpoolRetryGivesUp(t *testing.T) {
	err := errors.New("bad")
	keyedErrorSubmitter := new(observability_mock.K8sKeyedErrorHealthCheckSource)
	keyedErrorSubmitter.On("Submit", mock.Anything, service1Elem.WitchcraftServiceName, err).Times(maxNumRequeues + 1)
	processor := new(workerpool_mock.ElementProcessor[testElement])
	processor.On("ProcessElement", mock.Anything, service1Elem).Return(err).Times(maxNumRequeues + 1)
	defer mock.AssertExpectationsForObjects(t, keyedErrorSubmitter, processor)

	ctx := testutil.GetTestContext()
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	workers := workerpool.NewDefaultWorkerPool[testElement](processor, 5, queue, keyedErrorSubmitter, "testPool")

	workers.Start(ctx)
	workers.Submit(ctx, service1Elem)
	time.Sleep(offsetDuration + baseDuration*(maxNumRequeues*2))
	queue.ShutDown()
}

func TestCanHandlePanicInProcessor(t *testing.T) {
	keyedErrorSubmitter := new(observability_mock.K8sKeyedErrorHealthCheckSource)
	errCheck := func(err error) bool {
		return err != nil
	}
	keyedErrorSubmitter.On("Submit", mock.Anything, service1Elem.WitchcraftServiceName, mock.MatchedBy(errCheck)).Times(6)
	panicProcessor := &panicProcessor{
		shouldPanic: true,
	}
	defer mock.AssertExpectationsForObjects(t, keyedErrorSubmitter)

	ctx := testutil.GetTestContext()
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	workers := workerpool.NewDefaultWorkerPool[testElement](panicProcessor, 1, queue, keyedErrorSubmitter, "testPool")
	workers.Start(ctx)
	workers.Submit(ctx, service1Elem)
	time.Sleep(time.Millisecond * 200)
	// Make sure that we are called 6 times, this ensures that a thread will not die and that an element will be retried
	assert.Equal(t, 6, panicProcessor.callCount)
	// Stop the panic and make sure we can process no error
	panicProcessor.shouldPanic = false
	keyedErrorSubmitter.On("Submit", mock.Anything, service1Elem.WitchcraftServiceName, nil).Times(1)
	workers.Submit(ctx, service1Elem)
	time.Sleep(time.Millisecond * 200)
	assert.Equal(t, 7, panicProcessor.callCount)
	assert.True(t, panicProcessor.finished)
	// Now ensure the shut down will no longer process
	queue.ShutDown()
	workers.Submit(ctx, service1Elem)
	time.Sleep(time.Millisecond * 200)
	assert.Equal(t, 7, panicProcessor.callCount)
}

type panicProcessor struct {
	callCount   int
	shouldPanic bool
	finished    bool
}

func (p *panicProcessor) ProcessElement(ctx context.Context, element testElement) error {
	p.callCount = p.callCount + 1
	if p.shouldPanic {
		panic("hit a panic")
	}
	p.finished = true
	return nil
}

func TestCanHandlePanicInQueueInternal(t *testing.T) {
	ctx := testutil.GetTestContext()
	panicQueue := panicQueue{}
	workers := workerpool.NewDefaultWorkerPool[testElement](nil, 1, &panicQueue, nil, "testPool")
	workers.Start(ctx)
	workers.Submit(ctx, service1Elem)
	time.Sleep(offsetDuration)
	assert.True(t, panicQueue.getCount > 1)
}

type panicQueue struct {
	getCount int
}

func (p *panicQueue) ShutDownWithDrain() {
	panic("implement me")
}

func (p *panicQueue) Add(item interface{}) {}

func (p *panicQueue) Len() int {
	panic("implement me")
}

func (p *panicQueue) Get() (item interface{}, shutdown bool) {
	p.getCount = p.getCount + 1
	panic("nope")
}

func (p *panicQueue) Done(item interface{}) {
	panic("implement me")
}

func (p *panicQueue) ShutDown() {
	panic("implement me")
}

func (p *panicQueue) ShuttingDown() bool {
	return false
}

func (p *panicQueue) AddAfter(item interface{}, duration time.Duration) {
	panic("implement me")
}

func (p *panicQueue) AddRateLimited(item interface{}) {
	panic("implement me")
}

func (p *panicQueue) Forget(item interface{}) {
	panic("implement me")
}

func (p *panicQueue) NumRequeues(item interface{}) int {
	panic("implement me")
}
