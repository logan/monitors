package tally

import (
	"reflect"
	"testing"
)

func TestSnapshots(t *testing.T) {
	parent := NewSnapshot()
	a := NewSnapshot()
	b := NewSnapshot()
	a.Count("x", 1)
	a.Count("y", 2)
	a.Count("tallier.messages.child_1", 2)
	a.Count("tallier.bytes.child_1", 20)
	b.Count("y", 3)
	b.Count("z", 4)
	b.Count("tallier.messages.child_2", 3)
	b.Count("tallier.bytes.child_2", 30)

	expected := NewSnapshot()
	expected.Count("x", 1)
	expected.Count("y", 5)
	expected.Count("z", 4)
	expected.Count("tallier.messages.child_1", 2)
	expected.Count("tallier.bytes.child_1", 20)
	expected.Count("tallier.messages.child_2", 3)
	expected.Count("tallier.bytes.child_2", 30)
	expected.Count("tallier.messages.total", 5)
	expected.Count("tallier.bytes.total", 50)
	expected.numChildren = 2
	parent.Aggregate(a)
	parent.Aggregate(b)
	if !reflect.DeepEqual(expected, parent) {
		t.Errorf("expected %#v, got %#v", expected, parent)
	}
}
