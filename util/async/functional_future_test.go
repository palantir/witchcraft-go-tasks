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

package async

import (
	"context"
	"testing"

	werror "github.com/palantir/witchcraft-go-error"
	"github.com/stretchr/testify/assert"
)

func Test_FunctionalFuture(t *testing.T) {
	noErrorResult := NewFunctionalFuture(func(ctx context.Context) (int, error) {
		return 1, nil
	})
	errorResult := NewFunctionalFuture(func(ctx context.Context) (int, error) {
		return 0, werror.Error("err")
	})
	r, err := noErrorResult.Get(context.Background())
	assert.Equal(t, r, 1)
	assert.NoError(t, err)
	r, err = errorResult.Get(context.Background())
	assert.Equal(t, r, 0)
	assert.Error(t, err)
}
