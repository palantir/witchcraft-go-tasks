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

// ItemSubmitterConstraint defines the type requirements for items processed by ItemSubmitter.
// Items must be comparable (for deduplication) and implement fmt.Stringer (for health keys).
type ItemSubmitterConstraint interface {
	comparable
	fmt.Stringer
}

// ItemSubmitter provides a fire-and-forget interface for submitting items to be processed
// asynchronously with automatic retry, deduplication, and health reporting.
//
// Items are placed in a collapsing queue that deduplicates entries - if the same item is
// submitted multiple times while queued or being processed, duplicates are collapsed. A
// background goroutine pulls items and delegates processing to a ConsumerWorkerPool.
//
// On failure, items are requeued with exponential backoff (500ms base, 10s max) up to
// maxNumRequeues times (default 5), or indefinitely when configured with
// WithUnlimitedRetries. Processing results are reported to a KeyedErrorHealthCheckSource
// using each item's String() value as the key.
//
// Items must implement comparable (for deduplication) and fmt.Stringer (for health keys).
type ItemSubmitter[T ItemSubmitterConstraint] interface {
	// Submit adds an item to the queue for eventual processing. Returns immediately;
	// processing happens asynchronously. Duplicate submissions are collapsed.
	Submit(context.Context, T)
}

// DelayedItemSubmitter extends ItemSubmitter with keyed delayed submission. Delayed submissions
// for the same item are collapsed while waiting, with the earliest requested deadline taking
// precedence.
type DelayedItemSubmitter[T ItemSubmitterConstraint] interface {
	ItemSubmitter[T]
	// SubmitAfter adds an item to the queue after the provided delay. Returns immediately.
	SubmitAfter(context.Context, T, time.Duration)
}

type defaultItemSubmitter[T ItemSubmitterConstraint] struct {
	consumerWorkerPool          workerpool.ConsumerWorkerPool[T]
	keyedErrorHealthCheckSource window.KeyedErrorHealthCheckSource
	queue                       queue.CollapsingQueue[T]
	config                      ItemSubmitterConfig
}

// NewDefaultItemSubmitter creates a new ItemSubmitter and starts its background processing
// goroutine. The goroutine runs until ctx is cancelled.
func NewDefaultItemSubmitter[T ItemSubmitterConstraint](
	ctx context.Context,
	consumerWorkerPool workerpool.ConsumerWorkerPool[T],
	keyedErrorHealthCheckSource window.KeyedErrorHealthCheckSource,
	options ...ItemSubmitterOption) DelayedItemSubmitter[T] {
	config := ItemSubmitterConfig{
		maxNumRequeues: 5,
		logError: func(ctx context.Context, err error) {
			svc1log.FromContext(ctx).Error("error occurred processing element in workerpool", svc1log.Stacktrace(err))
		},
	}
	for _, option := range options {
		option(&config)
	}
	if config.itemSubmitterName != "" {
		newTag, err := metrics.NewTag("itemsubmittername", config.itemSubmitterName)
		if err == nil {
			config.additionalMetricTags = []metrics.Tag{newTag}
		}
	}
	defaultItemSubmitterArg := &defaultItemSubmitter[T]{
		consumerWorkerPool:          consumerWorkerPool,
		keyedErrorHealthCheckSource: keyedErrorHealthCheckSource,
		queue:                       queue.NewCollapsingQueue[T](),
		config:                      config,
	}
	go defaultItemSubmitterArg.startPullingFromQueue(ctx)
	go func() {
		<-ctx.Done()
		defaultItemSubmitterArg.queue.ShutDown()
	}()
	return defaultItemSubmitterArg
}

func (d defaultItemSubmitter[T]) Submit(ctx context.Context, element T) {
	d.queue.Add(element)
}

func (d defaultItemSubmitter[T]) SubmitAfter(ctx context.Context, element T, delay time.Duration) {
	d.queue.AddAfter(element, delay)
}

func (d defaultItemSubmitter[T]) startPullingFromQueue(ctx context.Context) {
	for {
		element, shutdown := d.queue.Get()
		if shutdown {
			svc1log.FromContext(ctx).Warn("Queue shutting down; workers stopping.")
			return
		}
		d.singleProcessAttempt(ctx, element)
		metrics.FromContext(ctx).Gauge("com.palantir.witchcraft.queue_length", d.config.additionalMetricTags...).Update(int64(d.queue.Len()))
	}
}

func (d defaultItemSubmitter[T]) singleProcessAttempt(ctx context.Context, element T) {
	startTime := time.Now()
	span, ctx := wtracing.StartSpanFromTracerInContext(ctx, "defaultItemSubmitter.runSingleJob")
	defer span.Finish()
	params := []svc1log.Param{
		svc1log.SafeParam("queueElementIdentifier", element.String()),
	}
	if d.config.itemSubmitterName != "" {
		params = append(params, svc1log.SafeParam("itemSubmitterName", d.config.itemSubmitterName))
	}
	ctx = svc1log.WithLoggerParams(ctx, params...)
	d.consumerWorkerPool.SubmitWithCallback(ctx, element, func(ctx context.Context, elem T, err error) {
		metrics.FromContext(ctx).Timer("com.palantir.witchcraft.process_element_duration", d.config.additionalMetricTags...).UpdateSince(startTime)
		d.keyedErrorHealthCheckSource.Submit(ctx, elem.String(), err)
		d.queue.Done(element)
		if err != nil {
			d.handleProcessError(ctx, element, err)
			return
		}
		d.queue.ResetRateLimit(element)
	})
}

func (d defaultItemSubmitter[T]) handleProcessError(ctx context.Context, element T, err error) {
	numRequeues := d.queue.NumRequeues(element)
	ctx = svc1log.WithLoggerParams(ctx,
		svc1log.SafeParam("maxNumRequeues", d.config.maxNumRequeues),
		svc1log.SafeParam("numRequeues", numRequeues),
		svc1log.SafeParam("unlimitedRetries", d.config.unlimitedRetries),
	)
	d.config.logError(ctx, err)
	if !d.config.unlimitedRetries && numRequeues >= d.config.maxNumRequeues {
		d.queue.ResetRateLimit(element)
	} else {
		d.queue.AddRateLimited(element)
	}
}
