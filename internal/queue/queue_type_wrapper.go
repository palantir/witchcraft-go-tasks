package workqueue

// queue is a slice which implements Queue.
type queue[T comparable] []T

func (q *queue[T]) Touch(item T) {}

func (q *queue[T]) Push(item T) {
	*q = append(*q, item)
}

func (q *queue[T]) Len() int {
	return len(*q)
}

func (q *queue[T]) Pop() (item T) {
	item = (*q)[0]
	// The underlying array still exists and reference this object, so the object will not be garbage collected.
	(*q)[0] = *new(T)
	*q = (*q)[1:]
	return item
}
