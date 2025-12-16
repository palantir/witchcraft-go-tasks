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
)

// Future is the base asynchronous interface
// It is typed by its return type of T
type Future[T any] interface {
	Get(ctx context.Context) (T, error)
}

// VoidFuture is short hand for a Future in which we don't care about the return type
// Instead of defining a new Future with a signature, Get(ctx context.Context) (error)
// we just used the struct{} instead. The avoids all interfaces that deal with Futures having to be defined multiple times
// It is inspired by Java's Future<Void>
type VoidFuture = Future[struct{}]
