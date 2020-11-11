/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
Adapted from RocksDB inline skiplist.

Key differences:
- No optimization for sequential inserts (no "prev").
- No custom comparator.
- Support overwrites. This requires care when we see the same key when inserting.
  For RocksDB or LevelDB, overwrites are implemented as a newer sequence number in the key, so
	there is no need for values. We don't intend to support versioning. In-place updates of values
	would be more efficient.
- We discard all non-concurrent code.
- We do not support Splices. This simplifies the code a lot.
- No AllocateNode or other pointer arithmetic.
- We combine the findLessThan, findGreaterOrEqual, etc into one function.
*/

package skl

import (
	"bytes"
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/dgraph-io/badger/v2/y"
	"github.com/dgraph-io/ristretto/z"
)

const (
	maxHeight      = 20
	heightIncrease = math.MaxUint32 / 3
)

// MaxNodeSize is the memory footprint of a node of maximum height.
const MaxNodeSize = int(unsafe.Sizeof(node{}))

type node struct {
	// Multiple parts of the value are encoded as a single uint64 so that it
	// can be atomically loaded and stored:
	//   value offset: uint32 (bits 0-31)
	//   value size  : uint16 (bits 32-63)
	value uint64

	// A byte slice is 24 bytes. We are trying to save space here.
	keyOffset uint32 // Immutable. No need to lock to access key.
	keySize   uint16 // Immutable. No need to lock to access key.

	// Height of the tower.
	height uint16

	// Most nodes do not need to use the full height of the tower, since the
	// probability of each successive level decreases exponentially. Because
	// these elements are never accessed, they do not need to be allocated.
	// Therefore, when a node is allocated in the arena, its memory footprint
	// is deliberately truncated to not include unneeded tower elements.
	//
	// All accesses to elements should use CAS operations, with no need to lock.
	tower [maxHeight]uint32
}

type comparatorFunc func([]byte, []byte) int

type Skiplist struct {
	height      int32 // Current height. 1 <= height <= kMaxHeight. CAS.
	headOff     uint32
	ref         int32
	arena       *Arena
	hasVersions bool
	comparator  comparatorFunc
	OnClose     func()
}

// IncrRef increases the refcount
func (s *Skiplist) IncrRef() {
	atomic.AddInt32(&s.ref, 1)
}

// DecrRef decrements the refcount, deallocating the Skiplist when done using it
func (s *Skiplist) DecrRef() {
	newRef := atomic.AddInt32(&s.ref, -1)
	if newRef > 0 {
		return
	}
	if s.OnClose != nil {
		s.OnClose()
	}
	// Indicate we are closed. Good for testing.  Also, lets GC reclaim memory. Race condition
	// here would suggest we are accessing skiplist when we are supposed to have no reference!
	s.arena = nil
}

func newNode(arena *Arena, key []byte, u uint64, height int) uint32 {
	// The base level is already allocated in the node struct.
	offset := arena.putNode(height)
	keyOffset := arena.putKey(key)
	node := arena.getNode(offset)
	node.keyOffset = keyOffset
	node.keySize = uint16(len(key))
	node.height = uint16(height)
	node.value = u
	return offset
}

func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}

// NewSkiplist makes a new empty skiplist, with a given arena size.
func NewSkiplist(arenaSize int64) *Skiplist {
	buf := z.Calloc(int(arenaSize))
	s := NewSkiplistWith(buf, true, nil)
	s.OnClose = func() {
		z.Free(s.arena.data)
	}
	return s
}

// NewSkiplistWithBuffer makes a new skiplist, with a given byte slice.
func NewSkiplistWith(buf []byte, hasVersions bool, grow func(uint32) []byte) *Skiplist {
	arena := new(Arena)
	arena.data = buf
	arena.Grow = grow
	offset := arena.allocateValue(y.ValueStruct{})
	headoff := newNode(arena, nil, offset, maxHeight)
	sl := &Skiplist{
		height:      1,
		headOff:     headoff,
		arena:       arena,
		ref:         1,
		hasVersions: hasVersions,
		comparator:  bytes.Compare,
	}
	if sl.hasVersions {
		sl.comparator = y.CompareKeys
	}
	return sl
}

func (s *node) getValueOffset() (uint32, uint32) {
	value := atomic.LoadUint64(&s.value)
	return decodeValue(value)
}

func (s *node) key(arena *Arena) []byte {
	return arena.getKey(s.keyOffset, s.keySize)
}

func (s *node) setUint64(u uint64) {
	atomic.StoreUint64(&s.value, u)
}

func (s *node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&s.tower[h])
}

func (s *node) casNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&s.tower[h], old, val)
}

// Returns true if key is strictly > n.key.
// If n is nil, this is an "end" marker and we return false.
//func (s *Skiplist) keyIsAfterNode(key []byte, n *node) bool {
//	y.AssertTrue(n != s.head)
//	return n != nil && y.CompareKeys(key, n.key) > 0
//}

func (s *Skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && z.FastRand() <= heightIncrease {
		h++
	}
	return h
}

func (s *Skiplist) getNext(nd *node, height int) *node {
	return s.arena.getNode(nd.getNextOffset(height))
}

func (s *Skiplist) getHead() *node {
	return s.arena.getNode(s.headOff)
}

// findNear finds the node near to key.
// If less=true, it finds rightmost node such that node.key < key (if allowEqual=false) or
// node.key <= key (if allowEqual=true).
// If less=false, it finds leftmost node such that node.key > key (if allowEqual=false) or
// node.key >= key (if allowEqual=true).
// Returns the node found. The bool returned is true if the node has key equal to given key.
func (s *Skiplist) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	//x := s.head
	x := s.getHead()
	level := int(s.getHeight() - 1)
	for {
		// Assume x.key < key.
		next := s.getNext(x, level)
		if next == nil {
			// x.key < key < END OF LIST
			if level > 0 {
				// Can descend further to iterate closer to the end.
				level--
				continue
			}
			// Level=0. Cannot descend further. Let's return something that makes sense.
			if !less {
				return nil, false
			}
			// Try to return x. Make sure it is not a head node.
			if x == s.getHead() {
				return nil, false
			}
			return x, false
		}
		nextKey := next.key(s.arena)

		cmp := s.comparator(key, nextKey)
		if cmp > 0 {
			// x.key < next.key < key. We can continue to move right.
			x = next
			continue
		}
		if cmp == 0 {
			// x.key < key == next.key.
			if allowEqual {
				return next, true
			}
			if !less {
				// We want >, so go to base level to grab the next bigger note.
				return s.getNext(next, 0), false
			}
			// We want <. If not base level, we should go closer in the next level.
			if level > 0 {
				level--
				continue
			}
			// On base level. Return x.
			if x == s.getHead() {
				return nil, false
			}
			return x, false
		}
		// cmp < 0. In other words, x.key < key < next.
		if level > 0 {
			level--
			continue
		}
		// At base level. Need to return something.
		if !less {
			return next, false
		}
		// Try to return x. Make sure it is not a head node.
		if x == s.getHead() {
			return nil, false
		}
		return x, false
	}
}

// findSpliceForLevel returns (outBefore, outAfter) with outBefore.key <= key <= outAfter.key.
// The input "before" tells us where to start looking.
// If we found a node with the same key, then we return outBefore = outAfter.
// Otherwise, outBefore.key < key < outAfter.key.
func (s *Skiplist) findSpliceForLevel(key []byte, beforeOff uint32, level int) (uint32, uint32) {
	for {
		// Assume before.key < key.
		before := s.arena.getNode(beforeOff)
		nextOff := before.getNextOffset(level)
		next := s.arena.getNode(nextOff) //s.getNext(before, level)
		if next == nil {
			return beforeOff, nextOff
		}
		nextKey := next.key(s.arena)
		cmp := s.comparator(key, nextKey)
		if cmp == 0 {
			// Equality case.
			return nextOff, nextOff
		}
		if cmp < 0 {
			// before.key < key < next.key. We are done for this level.
			return beforeOff, nextOff
		}
		beforeOff = nextOff
		// before = next // Keep moving right on this level.
	}
}

func (s *Skiplist) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

// Put inserts the key-value pair.
func (s *Skiplist) Put(key []byte, v y.ValueStruct) {
	val := s.arena.allocateValue(v)
	s.PutUint64(key, val)
}

// PutUint64 inserts the key-value pair, with a uint64 value.
func (s *Skiplist) PutUint64(key []byte, u uint64) {
	// Since we allow overwrite, we may not need to create a new node. We might not even need to
	// increase the height. Let's defer these actions.

	listHeight := s.getHeight()
	var prev [maxHeight + 1]uint32
	var next [maxHeight + 1]uint32
	prev[listHeight] = s.headOff
	next[listHeight] = 0
	for i := int(listHeight) - 1; i >= 0; i-- {
		// Use higher level to speed up for current level.
		prev[i], next[i] = s.findSpliceForLevel(key, prev[i+1], i)
		if prev[i] == next[i] {
			nd := s.arena.getNode(prev[i])
			nd.setUint64(u)
			return
		}
	}

	// We do need to create a new node.
	height := s.randomHeight()
	xo := newNode(s.arena, key, u, height)
	x := s.arena.getNode(xo)
	// Try to increase s.height via CAS.
	listHeight = s.getHeight()
	for height > int(listHeight) {
		if atomic.CompareAndSwapInt32(&s.height, listHeight, int32(height)) {
			// Successfully increased skiplist.height.
			break
		}
		listHeight = s.getHeight()
	}

	// We always insert from the base level and up. After you add a node in base level, we cannot
	// create a node in the level above because it would have discovered the node in the base level.
	for i := 0; i < height; i++ {
		for {
			nd := s.arena.getNode(prev[i])
			if nd == nil {
				y.AssertTrue(i > 1) // This cannot happen in base level.
				// We haven't computed prev, next for this level because height exceeds old listHeight.
				// For these levels, we expect the lists to be sparse, so we can just search from head.
				prev[i], next[i] = s.findSpliceForLevel(key, s.headOff, i)
				// Someone adds the exact same key before we are able to do so. This can only happen on
				// the base level. But we know we are not on the base level.
				y.AssertTrue(prev[i] != next[i])
			}
			x.tower[i] = next[i]
			pnode := s.arena.getNode(prev[i])
			if pnode.casNextOffset(i, next[i], s.arena.getNodeOffset(x)) {
				// Managed to insert x between prev[i] and next[i]. Go to the next level.
				break
			}
			// CAS failed. We need to recompute prev and next.
			// It is unlikely to be helpful to try to use a different level as we redo the search,
			// because it is unlikely that lots of nodes are inserted between prev[i] and next[i].
			prev[i], next[i] = s.findSpliceForLevel(key, prev[i], i)
			if prev[i] == next[i] {
				y.AssertTruef(i == 0, "Equality can happen only on base level: %d", i)
				pnode := s.arena.getNode(prev[i])
				pnode.setUint64(u)
				return
			}
		}
	}
}

// Empty returns if the Skiplist is empty.
func (s *Skiplist) Empty() bool {
	return s.findLast() == nil
}

// findLast returns the last element. If head (empty list), we return nil. All the find functions
// will NEVER return the head nodes.
func (s *Skiplist) findLast() *node {
	n := s.getHead()
	level := int(s.getHeight()) - 1
	for {
		next := s.getNext(n, level)
		if next != nil {
			n = next
			continue
		}
		if level == 0 {
			if n == s.getHead() {
				return nil
			}
			return n
		}
		level--
	}
}

// Get gets the value associated with the key. It returns a valid value if it finds equal or earlier
// version of the same key.
func (s *Skiplist) Get(key []byte) y.ValueStruct {
	n, version := s.getInternal(key)
	if n == nil {
		return y.ValueStruct{}
	}
	valOffset, valSize := n.getValueOffset()
	vs := s.arena.getVal(valOffset, valSize)
	vs.Version = version
	return vs

}

func (s *Skiplist) GetUint64(key []byte) (uint64, bool) {
	n, _ := s.getInternal(key)
	if n == nil {
		return 0, false
	}
	return n.value, true
}

// getInternal finds the node which is greater than or equal to the given key
// from the skiplist. It returns the fetched node and it's version.
func (s *Skiplist) getInternal(key []byte) (*node, uint64) {
	n, _ := s.findNear(key, false, true) // findGreaterOrEqual.
	if n == nil {
		return nil, 0
	}
	fetchedKey := s.arena.getKey(n.keyOffset, n.keySize)
	// If the node has version, it means it is a badger kv and we should
	// compare it's key without the version.
	if s.hasVersions {
		if !y.SameKey(key, fetchedKey) {
			return nil, 0
		}
		version := y.ParseTs(fetchedKey)
		return n, version
	}

	// This is a key without version.
	if !bytes.Equal(key, fetchedKey) {
		return nil, 0
	}
	return n, 0
}

// NewIterator returns a skiplist iterator.  You have to Close() the iterator.
func (s *Skiplist) NewIterator() *Iterator {
	s.IncrRef()
	return &Iterator{list: s}
}

// MemSize returns the size of the Skiplist in terms of how much memory is used within its internal
// arena.
func (s *Skiplist) MemSize() int64 { return s.arena.size() }

// Iterator is an iterator over skiplist object. For new objects, you just
// need to initialize Iterator.list.
type Iterator struct {
	list *Skiplist
	n    *node
}

// Close frees the resources held by the iterator
func (s *Iterator) Close() error {
	s.list.DecrRef()
	return nil
}

// Valid returns true iff the iterator is positioned at a valid node.
func (s *Iterator) Valid() bool { return s.n != nil }

// Key returns the key at the current position.
func (s *Iterator) Key() []byte {
	return s.list.arena.getKey(s.n.keyOffset, s.n.keySize)
}

// Value returns value.
func (s *Iterator) Value() y.ValueStruct {
	valOffset, valSize := s.n.getValueOffset()
	return s.list.arena.getVal(valOffset, valSize)
}

// ValueUint64 returns the uint64 value of the current node.
func (s *Iterator) ValueUint64() uint64 {
	return s.n.value
}

// Next advances to the next position.
func (s *Iterator) Next() {
	y.AssertTrue(s.Valid())
	s.n = s.list.getNext(s.n, 0)
}

// Prev advances to the previous position.
func (s *Iterator) Prev() {
	y.AssertTrue(s.Valid())
	s.n, _ = s.list.findNear(s.Key(), true, false) // find <. No equality allowed.
}

// Seek advances to the first entry with a key >= target.
func (s *Iterator) Seek(target []byte) {
	s.n, _ = s.list.findNear(target, false, true) // find >=.
}

// SeekForPrev finds an entry with key <= target.
func (s *Iterator) SeekForPrev(target []byte) {
	s.n, _ = s.list.findNear(target, true, true) // find <=.
}

// SeekToFirst seeks position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *Iterator) SeekToFirst() {
	s.n = s.list.getNext(s.list.getHead(), 0)
}

// SeekToLast seeks position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *Iterator) SeekToLast() {
	s.n = s.list.findLast()
}

// UniIterator is a unidirectional memtable iterator. It is a thin wrapper around
// Iterator. We like to keep Iterator as before, because it is more powerful and
// we might support bidirectional iterators in the future.
type UniIterator struct {
	iter     *Iterator
	reversed bool
}

// NewUniIterator returns a UniIterator.
func (s *Skiplist) NewUniIterator(reversed bool) *UniIterator {
	return &UniIterator{
		iter:     s.NewIterator(),
		reversed: reversed,
	}
}

// Next implements y.Interface
func (s *UniIterator) Next() {
	if !s.reversed {
		s.iter.Next()
	} else {
		s.iter.Prev()
	}
}

// Rewind implements y.Interface
func (s *UniIterator) Rewind() {
	if !s.reversed {
		s.iter.SeekToFirst()
	} else {
		s.iter.SeekToLast()
	}
}

// Seek implements y.Interface
func (s *UniIterator) Seek(key []byte) {
	if !s.reversed {
		s.iter.Seek(key)
	} else {
		s.iter.SeekForPrev(key)
	}
}

// Key implements y.Interface
func (s *UniIterator) Key() []byte { return s.iter.Key() }

// Value implements y.Interface
func (s *UniIterator) Value() y.ValueStruct { return s.iter.Value() }

// Valid implements y.Interface
func (s *UniIterator) Valid() bool { return s.iter.Valid() }

// Close implements y.Interface (and frees up the iter's resources)
func (s *UniIterator) Close() error { return s.iter.Close() }
