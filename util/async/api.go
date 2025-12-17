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
// we just used the struct{} instead. This avoids all interfaces that deal with Futures having to be defined multiple times
// It is inspired by Java's Future<Void>
type VoidFuture = Future[struct{}]
