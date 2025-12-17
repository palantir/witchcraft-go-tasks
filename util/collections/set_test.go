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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	s := New[int]()
	assert.Equal(t, 0, len(s), "Expected new set to be empty")
}

func TestInsertAndHas(t *testing.T) {
	s := New[string]()
	s.Insert("apple")
	s.Insert("banana")

	assert.True(t, s.Has("apple"), "Set should contain 'apple'")
	assert.True(t, s.Has("banana"), "Set should contain 'banana'")
	assert.False(t, s.Has("cherry"), "Set should not contain 'cherry'")
}

func TestDelete(t *testing.T) {
	s := New[int]()
	s.Insert(1)
	s.Insert(2)
	s.Insert(3)

	s.Delete(2)

	assert.False(t, s.Has(2), "Set should not contain 2 after deletion")
	assert.True(t, s.Has(1), "Set should still contain 1")
	assert.True(t, s.Has(3), "Set should still contain 3")
}

func TestDifference(t *testing.T) {
	s1 := New[int]()
	s1.Insert(1)
	s1.Insert(2)
	s1.Insert(3)

	s2 := New[int]()
	s2.Insert(2)
	s2.Insert(4)

	expected := New[int]()
	expected.Insert(1)
	expected.Insert(3)

	diff := s1.Difference(s2)

	assert.Equal(t, expected, diff, "Difference result should match expected")
}

func TestIntersection(t *testing.T) {
	s1 := New[string]()
	s1.Insert("apple")
	s1.Insert("banana")
	s1.Insert("cherry")

	s2 := New[string]()
	s2.Insert("banana")
	s2.Insert("date")
	s2.Insert("fig")

	expected := New[string]()
	expected.Insert("banana")

	intersect := s1.Intersection(s2)

	assert.Equal(t, expected, intersect, "Intersection result should match expected")
}

func TestEmptyDifference(t *testing.T) {
	s1 := New[int]()
	s1.Insert(1)
	s1.Insert(2)

	s2 := New[int]()

	expected := New[int]()
	expected.Insert(1)
	expected.Insert(2)

	diff := s1.Difference(s2)

	assert.Equal(t, expected, diff, "Difference with empty set should return the original set")
}

func TestEmptyIntersection(t *testing.T) {
	s1 := New[int]()
	s1.Insert(1)
	s1.Insert(2)

	s2 := New[int]()

	expected := New[int]()

	intersect := s1.Intersection(s2)

	assert.Equal(t, expected, intersect, "Intersection with empty set should be empty")
}

func TestSelfDifference(t *testing.T) {
	s := New[int]()
	s.Insert(1)
	s.Insert(2)

	expected := New[int]()

	diff := s.Difference(s)

	assert.Equal(t, expected, diff, "Difference of a set with itself should be empty")
}

func TestSelfIntersection(t *testing.T) {
	s := New[int]()
	s.Insert(1)
	s.Insert(2)

	expected := New[int]()
	expected.Insert(1)
	expected.Insert(2)

	intersect := s.Intersection(s)

	assert.Equal(t, expected, intersect, "Intersection of a set with itself should be the same set")
}

func TestInsertDuplicate(t *testing.T) {
	s := New[int]()
	s.Insert(1)
	s.Insert(1)

	assert.Equal(t, 1, len(s), "Set should only contain one instance of an inserted element")
}

func TestHasNonExistent(t *testing.T) {
	s := New[int]()
	s.Insert(1)
	s.Insert(2)

	assert.False(t, s.Has(3), "Set should not contain 3")
}

func TestDeleteNonExistent(t *testing.T) {
	s := New[int]()
	s.Insert(1)
	s.Delete(2) // Deleting non-existent element should do nothing

	assert.Equal(t, 1, len(s), "Set should still contain original elements after deleting non-existent element")
}

func TestToSlice(t *testing.T) {
	s := New[int]()
	s.Insert(1)
	s.Insert(2)
	s.Insert(3)

	slice := s.ToSlice()

	// ElementsMatch ignores the order of the elements.
	expected := []int{1, 2, 3}
	assert.ElementsMatch(t, expected, slice, "ToSlice should return a slice with all set elements")
}

func TestToSliceEmptySet(t *testing.T) {
	s := New[string]()

	slice := s.ToSlice()

	assert.Empty(t, slice, "ToSlice on an empty set should return an empty slice")
}

func TestEquals(t *testing.T) {
	s1 := New[int]()
	s1.Insert(1)
	s1.Insert(2)
	s1.Insert(3)

	s2 := New[int]()
	s2.Insert(3)
	s2.Insert(2)
	s2.Insert(1)

	assert.True(t, s1.Equals(s2), "Sets with the same elements should be equal")
}

func TestEqualsWithDifferentSets(t *testing.T) {
	s1 := New[int]()
	s1.Insert(1)
	s1.Insert(2)

	s2 := New[int]()
	s2.Insert(2)
	s2.Insert(3)

	assert.False(t, s1.Equals(s2), "Sets with different elements should not be equal")
}

func TestEqualsWithEmptySets(t *testing.T) {
	s1 := New[int]()
	s2 := New[int]()

	assert.True(t, s1.Equals(s2), "Two empty sets should be equal")
}

func TestEqualsWithDifferentSizes(t *testing.T) {
	s1 := New[int]()
	s1.Insert(1)

	s2 := New[int]()
	s2.Insert(1)
	s2.Insert(2)

	assert.False(t, s1.Equals(s2), "Sets of different sizes should not be equal")
}

func TestEqualsSelf(t *testing.T) {
	s := New[int]()
	s.Insert(1)
	s.Insert(2)

	assert.True(t, s.Equals(s), "A set should be equal to itself")
}

func TestEqualsEmptyAndNonEmptySet(t *testing.T) {
	s1 := New[int]()
	s2 := New[int]()
	s2.Insert(1)

	assert.False(t, s1.Equals(s2), "An empty set should not equal a non-empty set")
}
