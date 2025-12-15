package set

type empty struct{}

// Set is a simple set.
type Set[T comparable] map[T]empty

// New creates a new Set.
func New[T comparable]() Set[T] {
	return make(Set[T])
}

func (s Set[T]) Has(item T) bool {
	_, exists := s[item]
	return exists
}

func (s Set[T]) Insert(item T) {
	s[item] = empty{}
}

func (s Set[T]) Delete(item T) {
	delete(s, item)
}

func (s Set[T]) Len() int {
	return len(s)
}

// Difference returns a new set with elements in the current set that are not in the other set.
func (s Set[T]) Difference(other Set[T]) Set[T] {
	result := New[T]()
	for item := range s {
		if !other.Has(item) {
			result.Insert(item)
		}
	}
	return result
}

// Intersection returns a new set with elements that are in both sets.
func (s Set[T]) Intersection(other Set[T]) Set[T] {
	result := New[T]()
	for item := range s {
		if other.Has(item) {
			result.Insert(item)
		}
	}
	return result
}

// ToSlice returns a slice containing all elements of the set.
func (s Set[T]) ToSlice() []T {
	result := make([]T, len(s))
	idx := 0
	for item := range s {
		result[idx] = item
		idx++
	}
	return result
}

// Equals checks if two sets are equal, i.e., they contain the same elements.
func (s Set[T]) Equals(other Set[T]) bool {
	if len(s) != len(other) {
		return false
	}
	for item := range s {
		if !other.Has(item) {
			return false
		}
	}
	return true
}
