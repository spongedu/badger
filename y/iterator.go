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

package y

import (
	"github.com/pkg/errors"
)

// ValueStruct represents the value info that can be associated with a key, but also the internal
// Meta field.
type ValueStruct struct {
	Meta     byte
	UserMeta byte
	Value    []byte

	Version uint64 // This field is not serialized. Only for internal usage.
}

// EncodedSize is the size of the ValueStruct when encoded
func (v *ValueStruct) EncodedSize() uint16 {
	return uint16(len(v.Value) + 2) // meta, usermeta.
}

// Decode uses the length of the slice to infer the length of the Value field.
func (v *ValueStruct) Decode(b []byte) {
	v.Meta = b[0]
	v.UserMeta = b[1]
	v.Value = b[2:]
	v.Version = 0
}

// Encode expects a slice of length at least v.EncodedSize().
func (v *ValueStruct) Encode(b []byte) {
	b[0] = v.Meta
	b[1] = v.UserMeta
	copy(b[2:], v.Value)
}

// EncodeTo should be kept in sync with the Encode function above. The reason
// this function exists is to avoid creating byte arrays per key-value pair in
// table/builder.go.
func (v *ValueStruct) EncodeTo(buf []byte) []byte {
	buf = append(buf, v.Meta)
	buf = append(buf, v.UserMeta)
	buf = append(buf, v.Value...)
	return buf
}

// Iterator is an interface for a basic iterator.
type Iterator interface {
	Next()
	Rewind()
	Seek(key []byte)
	Key() []byte
	Value() ValueStruct
	FillValue(vs *ValueStruct)
	Valid() bool

	// All iterators should be closed so that file garbage collection works.
	Close() error
}

// MergeTowIterator is a specialized MergeIterator that only merge tow iterators.
// It is an optimization for compaction.
type MergeIterator struct {
	smaller mergeIteratorChild
	bigger  mergeIteratorChild
	second  Iterator
	reverse bool
}

type mergeIteratorChild struct {
	valid bool
	key   []byte
	iter  Iterator
	merge *MergeIterator
}

func (child *mergeIteratorChild) reset() {
	if child.merge != nil {
		child.valid = child.merge.smaller.valid
		if child.valid {
			child.key = child.merge.smaller.key
		}
	} else {
		child.valid = child.iter.Valid()
		if child.valid {
			child.key = child.iter.Key()
		}
	}
}

func (mt *MergeIterator) fix() {
	if !mt.bigger.valid {
		return
	}
	var cmp int
	for mt.smaller.valid {
		cmp = CompareKeys(mt.smaller.key, mt.bigger.key)
		if cmp == 0 {
			mt.second.Next()
			var secondValid bool
			if mt.second == mt.smaller.iter {
				mt.smaller.reset()
				secondValid = mt.smaller.valid
			} else {
				mt.bigger.reset()
				secondValid = mt.bigger.valid
			}
			if !secondValid {
				return
			}
			continue
		}
		if mt.reverse {
			if cmp < 0 {
				mt.swap()
			}
		} else {
			if cmp > 0 {
				mt.swap()
			}
		}
		return
	}
	mt.swap()
}

func (mt *MergeIterator) swap() {
	mt.smaller, mt.bigger = mt.bigger, mt.smaller
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (mt *MergeIterator) Next() {
	if mt.smaller.merge != nil {
		mt.smaller.merge.Next()
	} else {
		mt.smaller.iter.Next()
	}
	mt.smaller.reset()
	mt.fix()
}

// Rewind seeks to first element (or last element for reverse iterator).
func (mt *MergeIterator) Rewind() {
	mt.smaller.iter.Rewind()
	mt.smaller.reset()
	mt.bigger.iter.Rewind()
	mt.bigger.reset()
	mt.fix()
}

// Seek brings us to element with key >= given key.
func (mt *MergeIterator) Seek(key []byte) {
	mt.smaller.iter.Seek(key)
	mt.smaller.reset()
	mt.bigger.iter.Seek(key)
	mt.bigger.reset()
	mt.fix()
}

// Valid returns whether the MergeIterator is at a valid element.
func (mt *MergeIterator) Valid() bool {
	return mt.smaller.valid
}

// Key returns the key associated with the current iterator
func (mt *MergeIterator) Key() []byte {
	return mt.smaller.key
}

// Value returns the value associated with the iterator.
func (mt *MergeIterator) Value() ValueStruct {
	return mt.smaller.iter.Value()
}

func (mt *MergeIterator) FillValue(vs *ValueStruct) {
	if mt.smaller.merge != nil {
		mt.smaller.merge.FillValue(vs)
	} else {
		mt.smaller.iter.FillValue(vs)
	}
}

// Close implements y.Iterator
func (mt *MergeIterator) Close() error {
	err1 := mt.smaller.iter.Close()
	err2 := mt.bigger.iter.Close()
	if err1 != nil {
		return errors.Wrap(err1, "MergeIterator")
	}
	return errors.Wrap(err2, "MergeIterator")
}

// NewMergeIterator creates a merge iterator
func NewMergeIterator(iters []Iterator, reverse bool) Iterator {
	if len(iters) == 1 {
		return iters[0]
	} else if len(iters) == 2 {
		mi := &MergeIterator{
			second:  iters[1],
			reverse: reverse,
		}
		mi.smaller.iter = iters[0]
		mi.smaller.merge, _ = mi.smaller.iter.(*MergeIterator)
		mi.bigger.iter = iters[1]
		mi.bigger.merge, _ = mi.bigger.iter.(*MergeIterator)
		return mi
	}
	mid := len(iters) / 2
	return NewMergeIterator([]Iterator{NewMergeIterator(iters[:mid], reverse), NewMergeIterator(iters[mid:], reverse)}, reverse)
}
