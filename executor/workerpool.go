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
	"fmt"
	"time"

	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-health/v2/sources/window"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-tasks/internal/queue"
	"github.com/palantir/witchcraft-go-tasks/workerpool"
	"github.com/palantir/witchcraft-go-tracing/wtracing"
)

type constraint interface {
	comparable
	fmt.Stringer
}

// ElementProcessor is a wrapper around a rate limited queue backed by the K8s util library
type ElementProcessor[T constraint] interface {
	// Submit adds an element to the queue for eventual consumption
	Submit(context.Context, T)
}
type defaultWorkerPool[T constraint] struct {
	consumerWorkerPool             workerpool.ConsumerWorkerPool[T]
	k8sKeyedErrorHealthCheckSource window.KeyedErrorHealthCheckSource
	queue                          queue.CollapsingQueue[T]

	logError       func(ctx context.Context, err error)
	maxNumRequeues int
}

// NewDefaultWorkerPool instantiates a worker pool
func NewDefaultWorkerPool[T constraint](
	ctx context.Context,
	consumerWorkerPool workerpool.ConsumerWorkerPool[T],
	k8sKeyedErrorHealthCheckSource window.KeyedErrorHealthCheckSource,
	options ...JobOption[T]) ElementProcessor[T] {
	defaultWorkerPoolArg := &defaultWorkerPool[T]{
		consumerWorkerPool:             consumerWorkerPool,
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
	go defaultWorkerPoolArg.startPullingFromQueue(ctx)
	return defaultWorkerPoolArg
}

func (d defaultWorkerPool[T]) Submit(ctx context.Context, element T) {
	d.queue.Add(element)
}

func (d defaultWorkerPool[T]) startPullingFromQueue(ctx context.Context) {
	for {
		element, shutdown := d.queue.Get()
		if shutdown {
			svc1log.FromContext(ctx).Warn("Queue shutting down; workers stopping.")
			return
		}
		d.singleProcessAttempt(ctx, element)
		// TODO need NAME
		metrics.FromContext(ctx).Gauge("com.palantir.witchcraft.queue_length").Update(int64(d.queue.Len()))
	}
}

func (d defaultWorkerPool[T]) singleProcessAttempt(ctx context.Context, element T) {
	startTime := time.Now()
	span, ctx := wtracing.StartSpanFromTracerInContext(ctx, "defaultWorkerPool.runSingleJob")
	defer span.Finish()
	ctx = svc1log.WithLoggerParams(ctx, svc1log.SafeParam("queueElementIdentifier", element.String()))
	d.consumerWorkerPool.SubmitWithCallback(ctx, element, func(ctx context.Context, elem T, err error) {
		// TODO NEED NAME
		metrics.FromContext(ctx).Timer("com.palantir.witchcraft.process_element_duration").UpdateSince(startTime)
		d.k8sKeyedErrorHealthCheckSource.Submit(ctx, elem.String(), err)
		d.queue.Done(element)
		if err != nil {
			d.handleProcessError(ctx, element, err)
			return
		}
		d.queue.Forget(element)
	})
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
