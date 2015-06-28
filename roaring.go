// Package roaring is an implementation of Roaring Bitmaps in Go.
// They provide fast compressed bitmap data structures (also called bitset).
// They are ideally suited to represent sets of integers over
// relatively small ranges.
// See http://roaringbitmap.org for details.
package roaring

import (
	"bytes"
	"io"
	"strconv"
)

// RoaringBitmap represents a compressed bitmap where you can add integers.
type RoaringBitmap struct {
	highlowcontainer roaringArray
}

// Write out a serialized version of this bitmap to stream
func (b *RoaringBitmap) WriteTo(stream io.Writer) (int, error) {
	return b.highlowcontainer.writeTo(stream)
}

// Read a serialized version of this bitmap from stream
func (b *RoaringBitmap) ReadFrom(stream io.Reader) (int, error) {
	return b.highlowcontainer.readFrom(stream)
}

// NewRoaringBitmap creates a new empty RoaringBitmap
func NewRoaringBitmap() *RoaringBitmap {
	return &RoaringBitmap{*newRoaringArray()}
}

// Clear removes all content from the RoaringBitmap and frees the memory
func (rb *RoaringBitmap) Clear() {
	rb.highlowcontainer = *newRoaringArray()
}

// ToArray creates a new slice containing all of the integers stored in the RoaringBitmap in sorted order
func (rb *RoaringBitmap) ToArray() []uint32 {
	array := make([]uint32, rb.GetCardinality())
	pos := 0
	pos2 := 0

	for pos < rb.highlowcontainer.size() {
		hs := toIntUnsigned(rb.highlowcontainer.getKeyAtIndex(pos)) << 16
		c := rb.highlowcontainer.getContainerAtIndex(pos)
		pos++
		c.fillLeastSignificant16bits(array, pos2, hs)
		pos2 += c.getCardinality()
	}
	return array
}

// GetSizeInBytes estimates the memory usage of the RoaringBitmap. Note that this
// might differ slightly from the amount of bytes required for persistent storage
func (rb *RoaringBitmap) GetSizeInBytes() uint64 {
	size := uint64(8)
	for i := 0; i < rb.highlowcontainer.size(); i++ {
		c := rb.highlowcontainer.getContainerAtIndex(i)
		size += uint64(2) + uint64(c.getSizeInBytes())
	}
	return size
}

// GetSerializedSizeInBytes computes the serialized size in bytes  the RoaringBitmap. It should correspond to the
// number of bytes written when invoking WriteTo
func (rb *RoaringBitmap) GetSerializedSizeInBytes() uint64 {
	return rb.highlowcontainer.serializedSizeInBytes()
}

// IntIterable allows you to iterate over the values in a RoaringBitmap
type IntIterable interface {
	HasNext() bool
	Next() uint32
}

type intIterator struct {
	pos              int
	hs               uint32
	iter             shortIterable
	highlowcontainer *roaringArray
}

// HasNext returns true if there are more integers to iterate over
func (ii *intIterator) HasNext() bool {
	return ii.pos < ii.highlowcontainer.size()
}

func (ii *intIterator) init() {
	if ii.highlowcontainer.size() > ii.pos {
		ii.iter = ii.highlowcontainer.getContainerAtIndex(ii.pos).getShortIterator()
		ii.hs = toIntUnsigned(ii.highlowcontainer.getKeyAtIndex(ii.pos)) << 16
	}
}

// Next returns the next integer
func (ii *intIterator) Next() uint32 {
	x := toIntUnsigned(ii.iter.next()) | ii.hs
	if !ii.iter.hasNext() {
		ii.pos = ii.pos + 1
		ii.init()
	}
	return x
}

func newIntIterator(a *RoaringBitmap) *intIterator {
	p := new(intIterator)
	p.pos = 0
	p.highlowcontainer = &a.highlowcontainer
	p.init()
	return p
}

// String creates a string representation of the RoaringBitmap
func (rb *RoaringBitmap) String() string {
	// inspired by https://github.com/fzandona/goroar/blob/master/roaringbitmap.go
	var buffer bytes.Buffer
	start := []byte("{")
	buffer.Write(start)
	i := rb.Iterator()
	for i.HasNext() {
		buffer.WriteString(strconv.Itoa(int(i.Next())))
		if i.HasNext() { // todo: optimize
			buffer.WriteString(",")
		}
	}
	buffer.WriteString("}")
	return buffer.String()
}

// Iterator creates a new IntIterable to iterate over the integers contained in the bitmap, in sorted order
func (rb *RoaringBitmap) Iterator() IntIterable {
	return newIntIterator(rb)
}

// Clone creates a copy of the RoaringBitmap
func (rb *RoaringBitmap) Clone() *RoaringBitmap {
	ptr := new(RoaringBitmap)
	ptr.highlowcontainer = *rb.highlowcontainer.clone()
	return ptr
}

// Contains returns true if the integer is contained in the bitmap
func (rb *RoaringBitmap) Contains(x uint32) bool {
	return contains(rb.highlowcontainer, x)
}

// Contains returns true if the integer is contained in the bitmap (this is a convenience method, the parameter is casted to uint32 and Contains is called)
func (rb *RoaringBitmap) ContainsInt(x int) bool {
	return rb.Contains(uint32(x))
}

// Equals returns true if the two bitmaps contain the same integers
func (rb *RoaringBitmap) Equals(o interface{}) bool {
	srb, ok := o.(*RoaringBitmap)
	if ok {
		return srb.highlowcontainer.equals(rb.highlowcontainer)
	}
	return false
}

// Add the integer x to the bitmap
func (rb *RoaringBitmap) Add(x uint32) {
	hb := highbits(x)
	i := rb.highlowcontainer.getIndex(hb)
	if i >= 0 {
		c := rb.highlowcontainer.getWritableContainerAtIndex(i)
		rb.highlowcontainer.setContainerAtIndex(i, c.add(lowbits(x)))
	} else {
		newac := newArrayContainer()
		rb.highlowcontainer.insertNewKeyValueAt(-i-1, hb, newac.add(lowbits(x)))
	}
}

// Add the integer x to the bitmap (convenience method: the parameter is casted to uint32 and we call Add)
func (rb *RoaringBitmap) AddInt(x int) {
	rb.Add(uint32(x))
}

// Remove the integer x from the bitmap
func (rb *RoaringBitmap) Remove(x uint32) {
	hb := highbits(x)
	i := rb.highlowcontainer.getIndex(hb)
	if i >= 0 {
		c := rb.highlowcontainer.getWritableContainerAtIndex(i).remove(lowbits(x))
		rb.highlowcontainer.setContainerAtIndex(i, c.remove(lowbits(x)))
		if rb.highlowcontainer.getContainerAtIndex(i).getCardinality() == 0 {
			rb.highlowcontainer.removeAtIndex(i)
		}
	}
}

// IsEmpty returns true if the RoaringBitmap is empty (it is faster than doing (GetCardinality() == 0))
func (rb *RoaringBitmap) IsEmpty() bool {
	return rb.highlowcontainer.size() == 0
}

// GetCardinality returns the number of integers contained in the bitmap
func (rb *RoaringBitmap) GetCardinality() uint64 {
	return cardinality(rb.highlowcontainer)
}

// Rank returns the number of integers that are smaller or equal to x (Rank(infinity) would be GetCardinality())
func (rb *RoaringBitmap) Rank(x uint32) uint32 {
	return rank(rb.highlowcontainer, x)
}

// Select returns the xth integer in the bitmap
func (rb *RoaringBitmap) Select(x uint32) (uint32, error) {
	return selectInt(rb.highlowcontainer, x)
}

// And computes the intersection between two bitmaps and stores the result in the current bitmap
func (rb *RoaringBitmap) And(x2 *RoaringBitmap) {
	pos1 := 0
	pos2 := 0
	intersectionsize := 0
	length1 := rb.highlowcontainer.size()
	length2 := x2.highlowcontainer.size()

main:
	for {
		if pos1 < length1 && pos2 < length2 {
			s1 := rb.highlowcontainer.getKeyAtIndex(pos1)
			s2 := x2.highlowcontainer.getKeyAtIndex(pos2)
			for {
				if s1 == s2 {
					c1 := rb.highlowcontainer.getWritableContainerAtIndex(pos1)
					c2 := x2.highlowcontainer.getContainerAtIndex(pos2)
					diff := c1.iand(c2)
					if diff.getCardinality() > 0 {
						rb.highlowcontainer.replaceKeyAndContainerAtIndex(intersectionsize, s1, diff)
						intersectionsize++
					}
					pos1++
					pos2++
					if (pos1 == length1) || (pos2 == length2) {
						break main
					}
					s1 = rb.highlowcontainer.getKeyAtIndex(pos1)
					s2 = x2.highlowcontainer.getKeyAtIndex(pos2)
				} else if s1 < s2 {
					pos1 = rb.highlowcontainer.advanceUntil(s2, pos1)
					if pos1 == length1 {
						break main
					}
					s1 = rb.highlowcontainer.getKeyAtIndex(pos1)
				} else { //s1 > s2
					pos2 = x2.highlowcontainer.advanceUntil(s1, pos2)
					if pos2 == length2 {
						break main
					}
					s2 = x2.highlowcontainer.getKeyAtIndex(pos2)
				}
			}
		} else {
			break
		}
	}
	rb.highlowcontainer.resize(intersectionsize)
}

// Xor computes the symmetric difference between two bitmaps and stores the result in the current bitmap
func (rb *RoaringBitmap) Xor(x2 *RoaringBitmap) {
	results := Xor(rb, x2) // Todo: could be computed in-place for reduced memory usage
	rb.highlowcontainer = results.highlowcontainer
}

// Or computes the union between two bitmaps and stores the result in the current bitmap
func (rb *RoaringBitmap) Or(x2 *RoaringBitmap) {
	results := Or(rb, x2) // Todo: could be computed in-place for reduced memory usage
	rb.highlowcontainer = results.highlowcontainer
}

// AndNot computes the difference between two bitmaps and stores the result in the current bitmap
func (rb *RoaringBitmap) AndNot(x2 *RoaringBitmap) {
	pos1 := 0
	pos2 := 0
	intersectionsize := 0
	length1 := rb.highlowcontainer.size()
	length2 := x2.highlowcontainer.size()

main:
	for {
		if pos1 < length1 && pos2 < length2 {
			s1 := rb.highlowcontainer.getKeyAtIndex(pos1)
			s2 := x2.highlowcontainer.getKeyAtIndex(pos2)
			for {
				if s1 == s2 {
					c1 := rb.highlowcontainer.getWritableContainerAtIndex(pos1)
					c2 := x2.highlowcontainer.getContainerAtIndex(pos2)
					diff := c1.iandNot(c2)
					if diff.getCardinality() > 0 {
						rb.highlowcontainer.replaceKeyAndContainerAtIndex(intersectionsize, s1, diff)
						intersectionsize++
					}
					pos1++
					pos2++
					if (pos1 == length1) || (pos2 == length2) {
						break main
					}
					s1 = rb.highlowcontainer.getKeyAtIndex(pos1)
					s2 = x2.highlowcontainer.getKeyAtIndex(pos2)
				} else if s1 < s2 {
					c1 := rb.highlowcontainer.getWritableContainerAtIndex(pos1)
					rb.highlowcontainer.replaceKeyAndContainerAtIndex(intersectionsize, s1, c1)
					intersectionsize++
					pos1++
					if pos1 == length1 {
						break main
					}
					s1 = rb.highlowcontainer.getKeyAtIndex(pos1)
				} else { //s1 > s2
					pos2 = x2.highlowcontainer.advanceUntil(s1, pos2)
					if pos2 == length2 {
						break main
					}
					s2 = x2.highlowcontainer.getKeyAtIndex(pos2)
				}
			}
		} else {
			break
		}
	}
	// TODO:implement as a copy
	for pos1 < length1 {
		c1 := rb.highlowcontainer.getContainerAtIndex(pos1)
		s1 := rb.highlowcontainer.getKeyAtIndex(pos1)
		rb.highlowcontainer.replaceKeyAndContainerAtIndex(intersectionsize, s1, c1)
		intersectionsize++
		pos1++
	}
	rb.highlowcontainer.resize(intersectionsize)
}

// Or computes the union between two bitmaps and returns the result
func Or(x1, x2 *RoaringBitmap) *RoaringBitmap {
	return or(x1.highlowcontainer, x2.highlowcontainer)
}

// And computes the intersection between two bitmaps and returns the result
func And(x1, x2 *RoaringBitmap) *RoaringBitmap {
	return and(x1.highlowcontainer, x2.highlowcontainer)
}

// Xor computes the symmetric difference between two bitmaps and returns the result
func Xor(x1, x2 *RoaringBitmap) *RoaringBitmap {
	return xor(x1.highlowcontainer, x2.highlowcontainer)
}

// AndNot computes the difference between two bitmaps and returns the result
func AndNot(x1, x2 *RoaringBitmap) *RoaringBitmap {
	return andNot(x1.highlowcontainer, x2.highlowcontainer)
}

// BitmapOf generates a new bitmap filled with the specified integer
func BitmapOf(dat ...uint32) *RoaringBitmap {
	ans := NewRoaringBitmap()
	for _, i := range dat {
		ans.Add(i)
	}
	return ans
}

// Flip negates the bits in the given range, any integer present in this range and in the bitmap is removed,
// and any integer present in the range and not in the bitmap is added
func (rb *RoaringBitmap) Flip(rangeStart, rangeEnd uint32) {

	if rangeStart >= rangeEnd {
		return
	}

	hbStart := highbits(rangeStart)
	lbStart := lowbits(rangeStart)
	hbLast := highbits(rangeEnd - 1)
	lbLast := lowbits(rangeEnd - 1)

	max := toIntUnsigned(maxLowBit())
	for hb := hbStart; hb <= hbLast; hb++ {
		containerStart := uint32(0)
		if hb == hbStart {
			containerStart = toIntUnsigned(lbStart)
		}
		containerLast := max
		if hb == hbLast {
			containerLast = toIntUnsigned(lbLast)
		}

		i := rb.highlowcontainer.getIndex(hb)

		if i >= 0 {
			c := rb.highlowcontainer.getContainerAtIndex(i).inot(int(containerStart), int(containerLast))
			if c.getCardinality() > 0 {
				rb.highlowcontainer.setContainerAtIndex(i, c)
			} else {
				rb.highlowcontainer.removeAtIndex(i)
			}
		} else { // *think* the range of ones must never be
			// empty.
			rb.highlowcontainer.insertNewKeyValueAt(-i-1, hb, rangeOfOnes(int(containerStart), int(containerLast)))
		}
	}
}

// FlipInt calls Flip after casting the parameters to uint32 (convenience method)
func (rb *RoaringBitmap) FlipInt(rangeStart, rangeEnd int) {
	rb.Flip(uint32(rangeStart), uint32(rangeEnd))
}

// Add the integers in [rangeStart, rangeEnd) to the bitmap
func (rb *RoaringBitmap) AddRange(rangeStart, rangeEnd uint32) {
	if rangeStart >= rangeEnd {
		return
	}

	hbStart := toIntUnsigned(highbits(rangeStart))
	lbStart := toIntUnsigned(lowbits(rangeStart))
	hbLast := toIntUnsigned(highbits(rangeEnd - 1))
	lbLast := toIntUnsigned(lowbits(rangeEnd - 1))

	max := toIntUnsigned(maxLowBit())
	for hb := uint16(hbStart); hb <= uint16(hbLast); hb++ {
		containerStart := uint32(0)
		if hb == uint16(hbStart) {
			containerStart = lbStart
		}
		containerLast := max
		if hb == uint16(hbLast) {
			containerLast = lbLast
		}

		i := rb.highlowcontainer.getIndex(hb)

		if i >= 0 {
			c := rb.highlowcontainer.getContainerAtIndex(i).iaddRange(int(containerStart), int(containerLast+1))
			rb.highlowcontainer.setContainerAtIndex(i, c)
		} else { // *think* the range of ones must never be
			// empty.
			rb.highlowcontainer.insertNewKeyValueAt(-i-1, hb, rangeOfOnes(int(containerStart), int(containerLast)))
		}
	}
}

// Remove the integers in [rangeStart, rangeEnd) from the bitmap
func (rb *RoaringBitmap) RemoveRange(rangeStart, rangeEnd uint32) {
	if rangeStart >= rangeEnd {
		return
	}

	hbStart := toIntUnsigned(highbits(rangeStart))
	lbStart := toIntUnsigned(lowbits(rangeStart))
	hbLast := toIntUnsigned(highbits(rangeEnd - 1))
	lbLast := toIntUnsigned(lowbits(rangeEnd - 1))

	max := toIntUnsigned(maxLowBit())

	if hbStart == hbLast {
		i := rb.highlowcontainer.getIndex(uint16(hbStart))
		if i < 0 {
			return
		}
		c := rb.highlowcontainer.getContainerAtIndex(i).iremoveRange(int(lbStart), int(lbLast+1))
		if c.getCardinality() > 0 {
			rb.highlowcontainer.setContainerAtIndex(i, c)
		} else {
			rb.highlowcontainer.removeAtIndex(i)
		}
		return
	}
	ifirst := rb.highlowcontainer.getIndex(uint16(hbStart))
	ilast := rb.highlowcontainer.getIndex(uint16(hbLast))

	if ifirst >= 0 {
		if lbStart != 0 {
			c := rb.highlowcontainer.getContainerAtIndex(ifirst).iremoveRange(int(lbStart), int(max+1))
			if c.getCardinality() > 0 {
				rb.highlowcontainer.setContainerAtIndex(ifirst, c)
				ifirst++
			}
		}
	} else {
		ifirst = -ifirst - 1
	}
	if ilast >= 0 {
		if lbLast != max {
			c := rb.highlowcontainer.getContainerAtIndex(ilast).iremoveRange(int(0), int(lbLast+1))
			if c.getCardinality() > 0 {
				rb.highlowcontainer.setContainerAtIndex(ilast, c)
			} else {
				ilast++
			}
		} else {
			ilast++
		}
	} else {
		ilast = -ilast - 1
	}
	rb.highlowcontainer.removeIndexRange(ifirst, ilast)
}

// Flip negates the bits in the given range, any integer present in this range and in the bitmap is removed,
// and any integer present in the range and not in the bitmap is added, a new bitmap is returned leaving
// the current bitmap unchanged
func Flip(bm *RoaringBitmap, rangeStart, rangeEnd uint32) *RoaringBitmap {
	if rangeStart >= rangeEnd {
		return bm.Clone()
	}

	answer := NewRoaringBitmap()
	hbStart := highbits(rangeStart)
	lbStart := lowbits(rangeStart)
	hbLast := highbits(rangeEnd - 1)
	lbLast := lowbits(rangeEnd - 1)

	// copy the containers before the active area
	answer.highlowcontainer.appendCopiesUntil(bm.highlowcontainer, hbStart)

	max := toIntUnsigned(maxLowBit())
	for hb := hbStart; hb <= hbLast; hb++ {
		containerStart := uint32(0)
		if hb == hbStart {
			containerStart = toIntUnsigned(lbStart)
		}
		containerLast := max
		if hb == hbLast {
			containerLast = toIntUnsigned(lbLast)
		}

		i := bm.highlowcontainer.getIndex(hb)
		j := answer.highlowcontainer.getIndex(hb)

		if i >= 0 {
			c := bm.highlowcontainer.getContainerAtIndex(i).not(int(containerStart), int(containerLast))
			if c.getCardinality() > 0 {
				answer.highlowcontainer.insertNewKeyValueAt(-j-1, hb, c)
			}

		} else { // *think* the range of ones must never be
			// empty.
			answer.highlowcontainer.insertNewKeyValueAt(-j-1, hb,
				rangeOfOnes(int(containerStart), int(containerLast)))
		}
	}
	// copy the containers after the active area.
	answer.highlowcontainer.appendCopiesAfter(bm.highlowcontainer, hbLast)

	return answer
}

// FlipInt calls Flip after casting the parameters to uint32 (convenience method)
func FlipInt(bm *RoaringBitmap, rangeStart, rangeEnd int) *RoaringBitmap {
	return Flip(bm, uint32(rangeStart), uint32(rangeEnd))
}

// Snapshot returns a read only view of the bitmap which shares the same memory
// as the existing bitmap.???
func (bm *RoaringBitmap) Snapshot() *Snapshot {
	keys := make([]uint16, len(bm.highlowcontainer.keys))
	containers := make([]container, len(bm.highlowcontainer.containers))
	dirty := make([]bool, len(bm.highlowcontainer.dirty))

	copy(keys, bm.highlowcontainer.keys)
	copy(containers, bm.highlowcontainer.containers)

	bm.highlowcontainer.markAllDirty()

	return &Snapshot{
		highlowcontainer: roaringArray{
			keys:       keys,
			containers: containers,
			dirty:      dirty,
		},
	}
}
