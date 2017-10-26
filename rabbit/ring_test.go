package rabbit

import (
	"testing"
)

// For some reasons only queues-style stuff have actual tests in our projects %)

func TestRingBasic(t *testing.T) {
	ring := NewTagRing(10)
	if ring.Cap() != 10 {
		t.Fatal("Unexpected cap")
	}

	ring.Enqueue(0)
	if ring.Pick().(int) != 0 {
		t.Fatal("Unexpected value of first element")
	}

	for i := 1; i < 20; i++ {
		ring.Enqueue(i)
		if d := ring.Dequeue().(int); d != i-1 {
			t.Fatalf("Unexpected value %v on dequeue(%v expected)", d, i-1)
		}
	}

	if ring.Len() != 1 {
		t.Fatal("Unexpected len")
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("No panic on overflow")
		}
	}()
	for i := uint64(0); i < ring.Cap(); i++ {
		ring.Enqueue(0)
	}
}

func TestRingTags(t *testing.T) {
	ring := NewTagRing(10)
	tag := ring.Enqueue(0)
	if tag != 1 {
		t.Fatal("First tag is not 1")
	}

	ring.Enqueue(1)

	for i := 2; i < 20; i++ {
		tag = ring.Enqueue(i)
		ring.Dequeue()
	}

	if tag != 20 {
		t.Fatalf("Unexpected last tag value %v", tag)
	}

	if !ring.ContainsTag(tag) {
		t.Error("Contain check failed for last tag")
	}
	if !ring.ContainsTag(tag - 1) {
		t.Errorf("Contain check failed for last - 1 tag for ring len = %v", ring.Len())
	}
	if ring.ContainsTag(tag + 1) {
		t.Error("Contain check successed for last + 1 tag")
	}
	if ring.ContainsTag(tag - ring.Len()) {
		t.Error("Contain check successed for last - len tag")
	}
	if v := ring.Get(tag).(int); v != 19 {
		t.Errorf("Get by tag returns unexpected value %v(%v expected)", v, 19)
	}

	ring.Update(tag, 42)
	if v := ring.Get(tag).(int); v != 42 {
		t.Error("Update test failed")
	}
	if ring.Get(tag+1) != nil {
		t.Error("Acess by invalid tag returns non-nil value")
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("No panic on invalid update")
		}
	}()
	ring.Update(tag+1, 24)
}
