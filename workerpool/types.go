package workerpool

import (
	"context"
)

// ElementProcessor acts on the given element
type ElementProcessor[T ElementIdentifier] interface {
	ProcessElement(ctx context.Context, element T) error
}

// K8sWorkerPool is a wrapper around a rate limited queue backed by the K8s util library
type K8sWorkerPool[T ElementIdentifier] interface {
	// Start initializes the workers so they can start processing the queue
	Start(context.Context)
	// Submit adds an element to the queue for eventual consumption
	Submit(context.Context, T)
}

// ElementIdentifier is the type constraint that elements must pass to use a worker queue
type ElementIdentifier interface {
	// GetIdentifier should return a unique identifier that is used for logs and health
	GetIdentifier() string
}
