package roaring

type Snapshot struct {
	highlowcontainer roaringArray
}

func (s *Snapshot) Contains(x uint32) bool {
	return contains(s.highlowcontainer, x)
}

func (s *Snapshot) GetCardinality() uint64 {
	return cardinality(s.highlowcontainer)
}

func (s *Snapshot) Rank(x uint32) uint32 {
	return rank(s.highlowcontainer, x)
}

func (s *Snapshot) Select(x uint32) (uint32, error) {
	return selectInt(s.highlowcontainer, x)
}

func (s *Snapshot) And(rb *RoaringBitmap) *RoaringBitmap {
	return and(s.highlowcontainer, rb.highlowcontainer)
}

func (s *Snapshot) AndNot(rb *RoaringBitmap) *RoaringBitmap {
	return andNot(s.highlowcontainer, rb.highlowcontainer)
}

func (s *Snapshot) Xor(rb *RoaringBitmap) *RoaringBitmap {
	return xor(s.highlowcontainer, rb.highlowcontainer)
}

func (s *Snapshot) Or(rb *RoaringBitmap) *RoaringBitmap {
	return or(s.highlowcontainer, rb.highlowcontainer)
}
