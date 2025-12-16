package types

// ToPointer returns a pointer to the object passed in
func ToPointer[T any](t T) *T {
	return &t
}

// ToValue dereferences the pointer object passed in
func ToValue[T any](t *T) T {
	return *t
}

// Zero returns the zero value of the type param
// This is useful if you need to return the generic zero value of a type
func Zero[T any]() T {
	return *new(T)
}
