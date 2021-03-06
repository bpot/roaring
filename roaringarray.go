package roaring

import (
	"encoding/binary"
	"io"
)

type container interface {
	clone() container
	and(container) container
	iand(container) container // i stands for inplace
	andNot(container) container
	iandNot(container) container // i stands for inplace
	getCardinality() int
	rank(uint16) int
	add(uint16) container
	addRange(start, final int) container  // range is [firstOfRange,lastOfRange)
	iaddRange(start, final int) container // i stands for inplace, range is [firstOfRange,lastOfRange)
	remove(uint16) container
	not(start, final int) container               // range is [firstOfRange,lastOfRange]
	inot(firstOfRange, lastOfRange int) container // i stands for inplace, range is [firstOfRange,lastOfRange]
	xor(r container) container
	getShortIterator() shortIterable
	contains(i uint16) bool
	equals(i interface{}) bool
	fillLeastSignificant16bits(array []uint32, i int, mask uint32)
	or(r container) container
	ior(r container) container // i stands for inplace
	lazyIOR(r container) container
	getSizeInBytes() int
	removeRange(start, final int) container  // range is [firstOfRange,lastOfRange)
	iremoveRange(start, final int) container // i stands for inplace, range is [firstOfRange,lastOfRange)
	selectInt(uint16) int
	serializedSizeInBytes() int
	readFrom(io.Reader) (int, error)
	writeTo(io.Writer) (int, error)
}

// careful: range is [firstOfRange,lastOfRange]
func rangeOfOnes(start, last int) container {
	if (last - start + 1) > arrayDefaultMaxSize {
		return newBitmapContainerwithRange(start, last)
	}

	return newArrayContainerRange(start, last)
}

type element struct {
	key   uint16
	value container
}

func (e *element) clone() element {
	var c element
	c.key = e.key
	c.value = e.value.clone()
	return c
}

func newelement(key uint16, value container) *element {
	ptr := new(element)
	ptr.key = key
	ptr.value = value
	return ptr
}

type roaringArray struct {
	array []*element
}

func newRoaringArray() *roaringArray {
	return &roaringArray{make([]*element, 0, 0)}
}

func (ra *roaringArray) append(key uint16, value container) {
	ra.array = append(ra.array, newelement(key, value))
}

func (ra *roaringArray) appendCopy(sa roaringArray, startingindex int) {
	ra.array = append(ra.array, newelement(sa.array[startingindex].key, sa.array[startingindex].value.clone()))
}

func (ra *roaringArray) appendCopyMany(sa roaringArray, startingindex, end int) {
	for i := startingindex; i < end; i++ {
		ra.appendCopy(sa, i)
	}
}

func (ra *roaringArray) appendCopiesUntil(sa roaringArray, stoppingKey uint16) {
	for i := 0; i < sa.size(); i++ {
		if sa.array[i].key >= stoppingKey {
			break
		}
		ra.array = append(ra.array, newelement(sa.array[i].key, sa.array[i].value.clone()))
	}
}

func (ra *roaringArray) appendCopiesAfter(sa roaringArray, beforeStart uint16) {
	startLocation := sa.getIndex(beforeStart)
	if startLocation >= 0 {
		startLocation++
	} else {
		startLocation = -startLocation - 1
	}

	for i := startLocation; i < sa.size(); i++ {
		ra.array = append(ra.array, newelement(sa.array[i].key, sa.array[i].value.clone()))
	}
}

func (ra *roaringArray) removeIndexRange(begin, end int) {
	if end <= begin {
		return
	}
	r := end - begin
	copy(ra.array[begin:], ra.array[end:])
	for i := 1; i <= r; i++ {
		ra.array[len(ra.array)-i] = nil
	}
	ra.array = ra.array[:len(ra.array)-r]
}

func (ra *roaringArray) resize(newsize int) {
	for k := newsize; k < len(ra.array); k++ {
		ra.array[k] = nil
	}
	ra.array = ra.array[:newsize]
}

func (ra *roaringArray) clear() {
	ra.array = make([]*element, 0, 0)
}

func (ra *roaringArray) clone() *roaringArray {
	sa := new(roaringArray)
	sa.array = make([]*element, len(ra.array))
	for i := 0; i < len(ra.array); i++ {
		newElement := ra.array[i].clone()
		sa.array[i] = &newElement
	}
	return sa
}

func (ra *roaringArray) containsKey(x uint16) bool {
	return (ra.binarySearch(0, len(ra.array), x) >= 0)
}

func (ra *roaringArray) getContainer(x uint16) container {
	i := ra.binarySearch(0, len(ra.array), x)
	if i < 0 {
		return nil
	}
	return ra.array[i].value
}

func (ra *roaringArray) getContainerAtIndex(i int) container {
	return ra.array[i].value
}

func (ra *roaringArray) getIndex(x uint16) int {
	// before the binary search, we optimize for frequent cases
	size := len(ra.array)
	if (size == 0) || (ra.array[size-1].key == x) {
		return size - 1
	}
	return ra.binarySearch(0, size, x)
}

func (ra *roaringArray) getKeyAtIndex(i int) uint16 {
	return ra.array[i].key
}

func (ra *roaringArray) insertNewKeyValueAt(i int, key uint16, value container) {
	s := ra.array
	s = append(s, nil)
	copy(s[i+1:], s[i:])
	s[i] = newelement(key, value)
	ra.array = s
}

func (ra *roaringArray) remove(key uint16) bool {
	i := ra.binarySearch(0, len(ra.array), key)
	if i >= 0 { // if a new key
		ra.removeAtIndex(i)
		return true
	}
	return false
}

func (ra *roaringArray) removeAtIndex(i int) {
	a := ra.array
	copy(a[i:], a[i+1:])
	a[len(a)-1] = nil // or the zero value of T
	a = a[:len(a)-1]
	ra.array = a //should be the same reference i think
}

func (ra *roaringArray) setContainerAtIndex(i int, c container) {
	ra.array[i].value = c
}
func (ra *roaringArray) replaceKeyAndContainerAtIndex(i int, key uint16, c container) {
	ra.array[i].key = key
	ra.array[i].value = c
}

func (ra *roaringArray) size() int {
	return len(ra.array)
}

func (ra *roaringArray) binarySearch(begin, end int, key uint16) int {
	low := begin
	high := end - 1
	ikey := int(key)

	for low <= high {
		middleIndex := int(uint((low + high)) >> 1)
		middleValue := int(ra.array[middleIndex].key)

		if middleValue < ikey {
			low = middleIndex + 1
		} else if middleValue > ikey {
			high = middleIndex - 1
		} else {
			return middleIndex
		}
	}
	return -(low + 1)
}
func (ra *roaringArray) equals(o interface{}) bool {
	srb, ok := o.(roaringArray)
	if ok {

		if srb.size() != ra.size() {
			return false
		}
		for i := 0; i < srb.size(); i++ {
			oself := ra.array[i]
			other := srb.array[i]
			if oself.key != other.key || !oself.value.equals(other.value) {
				return false
			}
		}
		return true
	}
	return false
}

func (b *roaringArray) serializedSizeInBytes() uint64 {
	count := uint64(4 + 4)
	for _, item := range b.array {
		count = count + 4 + 4
		count = count + uint64(item.value.serializedSizeInBytes())
	}
	return count
}

func (b *roaringArray) writeTo(stream io.Writer) (int, error) {
	err := binary.Write(stream, binary.LittleEndian, uint32(serial_cookie))
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.LittleEndian, uint32(len(b.array)))
	if err != nil {
		return 0, err
	}
	for _, item := range b.array {
		err = binary.Write(stream, binary.LittleEndian, uint16(item.key))
		if err != nil {
			return 0, err
		}
		err = binary.Write(stream, binary.LittleEndian, uint16(item.value.getCardinality()-1))
		if err != nil {
			return 0, err
		}
	}
	startOffset := 4 + 4 + 4*len(b.array) + 4*len(b.array)
	for _, item := range b.array {
		err = binary.Write(stream, binary.LittleEndian, uint32(startOffset))
		if err != nil {
			return 0, err
		}
		startOffset += getSizeInBytesFromCardinality(item.value.getCardinality())
	}
	for _, item := range b.array {
		_, err := item.value.writeTo(stream)
		if err != nil {
			return 0, err
		}
	}
	return startOffset, nil
}

func (b *roaringArray) readFrom(stream io.Reader) (int, error) {
	var cookie uint32
	err := binary.Read(stream, binary.LittleEndian, &cookie)
	if err != nil {
		return 0, err
	}
	if cookie != serial_cookie {
		return 0, err
	}
	var size uint32
	err = binary.Read(stream, binary.LittleEndian, &size)
	if err != nil {
		return 0, err
	}
	keycard := make([]uint16, 2*size, 2*size)
	err = binary.Read(stream, binary.LittleEndian, keycard)
	if err != nil {
		return 0, err
	}
	offsets := make([]uint32, size, size)
	err = binary.Read(stream, binary.LittleEndian, offsets)
	if err != nil {
		return 0, err
	}
	offset := int(4 + 4 + 8*size)
	for i := uint32(0); i < size; i++ {
		c := int(keycard[2*i+1]) + 1
		offset += int(getSizeInBytesFromCardinality(c))
		if c > arrayDefaultMaxSize {
			nb := newBitmapContainer()
			nb.readFrom(stream)
			nb.cardinality = int(c)
			b.append(keycard[2*i], nb)
		} else {
			nb := newArrayContainerSize(int(c))
			nb.readFrom(stream)
			b.append(keycard[2*i], nb)
		}
	}
	return offset, nil
}

func (ra *roaringArray) advanceUntil(min uint16, pos int) int {
	lower := pos + 1

	if lower >= len(ra.array) || ra.array[lower].key >= min {
		return lower
	}

	spansize := 1

	for lower+spansize < len(ra.array) && ra.array[lower+spansize].key < min {
		spansize *= 2
	}
	var upper int
	if lower+spansize < len(ra.array) {
		upper = lower + spansize
	} else {
		upper = len(ra.array) - 1
	}

	if ra.array[upper].key == min {
		return upper
	}

	if ra.array[upper].key < min {
		// means
		// array
		// has no
		// item
		// >= min
		// pos = array.length;
		return len(ra.array)
	}

	// we know that the next-smallest span was too small
	lower += (spansize / 2)

	mid := 0
	for lower+1 != upper {
		mid = (lower + upper) / 2
		if ra.array[mid].key == min {
			return mid
		} else if ra.array[mid].key < min {
			lower = mid
		} else {
			upper = mid
		}
	}
	return upper

}
