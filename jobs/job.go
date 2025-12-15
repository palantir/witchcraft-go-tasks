package jobs

import (
	"context"
	"time"

	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-tasks/runnable"
)

type defaultJob struct {
	name             string
	runnable         runnable.Runnable
	interval         time.Duration
	logError         func(ctx context.Context, err error)
	startImmediately bool
}

// NewDefaultJob returns a default Job implementation with the provided name and runFunc
func NewDefaultJob(name string, runnable runnable.Runnable, options ...JobOption) Job {
	defaultJob := &defaultJob{
		name:     name,
		runnable: runnable,
		interval: time.Minute,
		logError: func(ctx context.Context, err error) {
			svc1log.FromContext(ctx).Error("Encountered unexpected error during job execution", svc1log.Stacktrace(err))
		},
		startImmediately: false,
	}
	for _, option := range options {
		option.apply(defaultJob)
	}
	return defaultJob
}

func (d *defaultJob) ShouldStartImmediately(ctx context.Context) bool {
	return d.startImmediately
}

func (d *defaultJob) GetInterval(ctx context.Context) time.Duration {
	return d.interval
}

func (d *defaultJob) Name() string {
	return d.name
}

func (d *defaultJob) Run(ctx context.Context) error {
	return d.runnable.Run(ctx)
}

func (d *defaultJob) LogError(ctx context.Context, err error) {
	d.logError(ctx, err)
}
