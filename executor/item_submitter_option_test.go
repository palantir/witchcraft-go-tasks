// Copyright (c) 2026 Palantir Technologies. All rights reserved.
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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestItemSubmitterRetryOptionsUseLastAppliedMode(t *testing.T) {
	t.Run("maximum requeues overrides unlimited retries", func(t *testing.T) {
		config := WithMaxNumRequeues(2)(WithUnlimitedRetries()(&ItemSubmitterConfig{}))

		assert.Equal(t, &ItemSubmitterConfig{
			maxNumRequeues: 2,
		}, config)
	})

	t.Run("unlimited retries overrides maximum requeues", func(t *testing.T) {
		config := WithUnlimitedRetries()(WithMaxNumRequeues(2)(&ItemSubmitterConfig{}))

		assert.Equal(t, &ItemSubmitterConfig{
			maxNumRequeues:   2,
			unlimitedRetries: true,
		}, config)
	})
}
