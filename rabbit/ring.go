package rabbit

type TagRing struct {
	data   []interface{}
	curTag uint64
	head   uint64
	tail   uint64
	len    uint64
}

func NewTagRing(cap int) TagRing {
	return TagRing{
		data: make([]interface{}, cap, cap),
		// for reasons %)
		tail: 1,
	}
}

func (r TagRing) Cap() uint64 {
	return uint64(cap(r.data))
}

func (r TagRing) Len() uint64 {
	return uint64(r.len)
}

func (r TagRing) IsFull() bool {
	return r.Cap() == r.Len()
}

// Returns related tag, panic on capacity exceeded
func (r *TagRing) Enqueue(i interface{}) uint64 {
	if r.Len() == r.Cap() {
		panic("TagRing: capacity exceeded")
	}
	r.head = (r.head + 1) % r.Cap()
	r.curTag++
	r.len++
	r.data[r.head] = i
	return r.curTag
}

func (r *TagRing) Dequeue() interface{} {
	if r.len == 0 {
		return nil
	}
	oldTail := r.tail
	r.tail = (r.tail + 1) % r.Cap()
	r.len--
	return r.data[oldTail]
}

func (r TagRing) Pick() interface{} {
	if r.len == 0 {
		return nil
	}
	return r.data[r.tail]
}

func (r TagRing) Get(tag uint64) interface{} {
	if !r.ContainsTag(tag) {
		return nil
	}
	return r.data[tag%r.Cap()]
}

func (r *TagRing) Update(tag uint64, i interface{}) {
	if !r.ContainsTag(tag) {
		panic("TagRing: tag value outside bounds")
	}
	r.data[tag%r.Cap()] = i
}

// Tag of oldest element in ring.
// Returns 0 when ring is empty.
func (r TagRing) FirstTag() uint64 {
	if r.len == 0 {
		return 0
	}
	return r.curTag - r.Len() + 1
}

func (r TagRing) ContainsTag(tag uint64) bool {
	return tag <= r.curTag && tag > (r.curTag-r.Len())
}
