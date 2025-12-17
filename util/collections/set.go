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

package collections

import (
	"iter"
)

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

// Iterator returns an iterator that returns the elements in the set.
// The order of the returned elements is random.
func (s Set[T]) Iterator() iter.Seq[T] {
	return func(yield func(T) bool) {
		for k := range s {
			if !yield(k) {
				return
			}
		}
	}
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
