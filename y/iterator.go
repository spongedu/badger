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
	Valid() bool

	// All iterators should be closed so that file garbage collection works.
	Close() error
}

// MergeTowIterator is a specialized MergeIterator that only merge tow iterators.
// It is an optimization for compaction.
type MergeIterator struct {
	smallerValid bool
	biggerValid  bool
	smallerKey   []byte
	biggerKey    []byte
	smallerVal   ValueStruct
	biggerVal    ValueStruct

	second  Iterator
	smaller Iterator
	bigger  Iterator
	reverse bool
}

func (mt *MergeIterator) fix() {
	mt.resetSmaller()
	if !mt.biggerValid {
		return
	}
	var cmp int
	for mt.smallerValid {
		cmp = CompareKeys(mt.smallerKey, mt.biggerKey)
		if cmp == 0 {
			mt.second.Next()
			var secondValid bool
			if mt.second == mt.smaller {
				mt.resetSmaller()
				secondValid = mt.smallerValid
			} else {
				mt.resetBigger()
				secondValid = mt.biggerValid
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
	mt.smallerKey, mt.biggerKey = mt.biggerKey, mt.smallerKey
	mt.smallerValid, mt.biggerValid = mt.biggerValid, mt.smallerValid
	mt.smallerVal, mt.biggerVal = mt.biggerVal, mt.smallerVal
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (mt *MergeIterator) Next() {
	mt.smaller.Next()
	mt.fix()
}

func (mt *MergeIterator) resetBigger() {
	mt.biggerValid = mt.bigger.Valid()
	if mt.biggerValid {
		mt.biggerKey = mt.bigger.Key()
		mt.biggerVal = mt.bigger.Value()
	}
}

func (mt *MergeIterator) resetSmaller() {
	mt.smallerValid = mt.smaller.Valid()
	if mt.smallerValid {
		mt.smallerKey = mt.smaller.Key()
		mt.smallerVal = mt.smaller.Value()
	}
}

// Rewind seeks to first element (or last element for reverse iterator).
func (mt *MergeIterator) Rewind() {
	mt.smaller.Rewind()
	mt.bigger.Rewind()
	mt.resetBigger()
	mt.fix()
}

// Seek brings us to element with key >= given key.
func (mt *MergeIterator) Seek(key []byte) {
	mt.smaller.Seek(key)
	mt.bigger.Seek(key)
	mt.resetBigger()
	mt.fix()
}

// Valid returns whether the MergeIterator is at a valid element.
func (mt *MergeIterator) Valid() bool {
	return mt.smallerValid
}

// Key returns the key associated with the current iterator
func (mt *MergeIterator) Key() []byte {
	return mt.smallerKey
}

// Value returns the value associated with the iterator.
func (mt *MergeIterator) Value() ValueStruct {
	return mt.smallerVal
}

// Close implements y.Iterator
func (mt *MergeIterator) Close() error {
	err1 := mt.smaller.Close()
	err2 := mt.bigger.Close()
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
		return &MergeIterator{smaller: iters[0], bigger: iters[1], second: iters[1], reverse: reverse}
	}
	mid := len(iters) / 2
	return NewMergeIterator([]Iterator{NewMergeIterator(iters[:mid], reverse), NewMergeIterator(iters[mid:], reverse)}, reverse)
}
