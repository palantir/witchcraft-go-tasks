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

package jobs

import (
	"context"
	"time"

	"github.com/palantir/witchcraft-go-tasks/runnable"
)

// Job is a simple interface for running an operation
type Job interface {
	runnable.Runnable
	ShouldStartImmediately(ctx context.Context) bool
	GetInterval(ctx context.Context) time.Duration
	// LogError is the called if the job returns an error
	LogError(ctx context.Context, err error)
}

// JobRunner is an interface for safely running all the specified jobs discussed above
type JobRunner interface {
	StartJobs(ctx context.Context, jobs []Job)
}
