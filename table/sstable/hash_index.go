package sstable

import (
	"encoding/binary"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"unsafe"

	"github.com/pingcap/badger/table/sstable/mph"

)

const (
	resultNoEntry  = 65535
	resultFallback = 65534
	maxBlockCnt    = 65533
)

type hashEntry struct {
	entryPosition
	hash uint64
}

type entryPosition struct {
	// blockIdx is the index of block which contains this key.
	blockIdx uint16

	// offset is the index of this key in block.
	offset uint8
}

func (e *entryPosition) encode() []byte {
	b := make([]byte, 3)
	binary.LittleEndian.PutUint16(b, e.blockIdx)
	b[2] = e.offset
	return b
}

func (e *entryPosition) decode(b []byte) {
	e.blockIdx = binary.LittleEndian.Uint16(b)
	e.offset = b[2]
}

func buildHashIndex(hashEntries []hashEntry, hashUtilRatio float32) []byte {
	if len(hashEntries) == 0 {
		return u32ToBytes(0)
	}

	numBuckets := uint32(float32(len(hashEntries)) / hashUtilRatio)
	buf := make([]byte, numBuckets*3+4)
	copy(buf, u32ToBytes(numBuckets))
	buckets := buf[4:]
	for i := 0; i < int(numBuckets); i++ {
		binary.LittleEndian.PutUint16(buckets[i*3:], resultNoEntry)
	}

	for _, h := range hashEntries {
		idx := h.hash % uint64(numBuckets)
		bucket := buckets[idx*3 : (idx+1)*3]
		blkIdx := binary.LittleEndian.Uint16(bucket[:2])
		if blkIdx == resultNoEntry {
			binary.LittleEndian.PutUint16(bucket[:2], h.blockIdx)
			bucket[2] = h.offset
		} else if blkIdx != h.blockIdx {
			binary.LittleEndian.PutUint16(bucket[:2], resultFallback)
		}
	}

	return buf
}

type hashIndex struct {
	buckets    []byte
	numBuckets int
}

func (i *hashIndex) readIndex(buf []byte) {
	i.numBuckets = int(bytesToU32(buf[:4]))
	i.buckets = buf[4:]
}

func (i *hashIndex) lookup(keyHash uint64) (uint32, uint8) {
	if i.buckets == nil {
		return resultFallback, 0
	}
	idx := keyHash % uint64(i.numBuckets)
	buf := i.buckets[idx*3:]
	blkIdx := binary.LittleEndian.Uint16(buf)
	return uint32(blkIdx), uint8(buf[2])
}

type MphEntry struct {
	entryPosition
	key []byte
}

type MphIndex struct {
	Table *mph.Table
	Entries   []entryPosition
}

func buildMphIndex(hashEntries []MphEntry) []byte {
	log.Warn("START TO BUILD MPH HASH. ", zap.Int("entryCnt", len(hashEntries)))
	defer func() {
		log.Warn("FINISH idx.Table.Build")
	}()
	if len(hashEntries) == 0 {
		return u32ToBytes(0)
	}

	idx := MphIndex{}

	keys := make([][]byte,0)
	entries := make([]entryPosition,0)

	for _, e := range hashEntries {
		keys = append(keys, e.key)
		entries = append(entries, e.entryPosition)
	}

	idx.Entries = entries
	log.Warn("START idx.Table.Build")
	idx.Table = mph.Build(keys)
	log.Warn("END idx.Table.Build")

	sz := uint64(len(hashEntries))

	// totalLen(8) + entryCnt(8) + Level0Mask(8) + Level1Mask(8) + Level0Size(8) +Level0(var) + Level1Size(8) +Level1(var) + 3*HashEntries
	var totalLen = uint64(8 + 8 + 8 + 8 + 8 + len(idx.Table.Level0) * 4 + 8 + len(idx.Table.Level0) * 4 + 3*len(hashEntries))
	buf := make([]byte, totalLen)

	// Total Len
	copy(buf, u64ToBytes(totalLen))

	// Entry Size
	copy(buf, u64ToBytes(sz))

	// MPH Level0 mask
	p := buf[8:]
	copy(p, u64ToBytes(uint64(idx.Table.Level0Mask)))

	// MPH Level1 mask
	p = p[8:]
	copy(p, u64ToBytes(uint64(idx.Table.Level1Mask)))

	// MPH level0[i]
	for i, _ := range idx.Table.Level0 {
		copy(p, u32ToBytes(idx.Table.Level0[i]))
		p = p[4:]
	}

	// MPH level1[i]
	for i, _ := range idx.Table.Level0 {
		copy(p, u32ToBytes(idx.Table.Level1[i]))
		p = p[4:]
	}

	// Hash Entries
	for _, e := range hashEntries {
		binary.LittleEndian.PutUint16(p[:2], e.blockIdx)
		p[2] = e.offset
		p = p[3:]
	}
	return buf
}

func (i *MphIndex) readIndex(buf []byte) {
	log.Warn("START read mphIndex. ")
	defer func() {
		log.Warn("END read index")
	}()
	i.Table = &mph.Table{}
	i.Entries = make([]entryPosition, 0)

	// TotalLen
	totalLen := bytesToU64(buf[:8])
	buf = buf[8:]
	log.Warn("", zap.Uint64("TOTALEN", totalLen))

	// Entry cnt
	entryCnt := bytesToU64(buf[:8])
	buf = buf[8:]
	log.Warn("", zap.Uint64("ENTRY CNT", entryCnt))

	// MPH level0 mask
	level0Mask := bytesToU64(buf[:8])
	buf = buf[8:]
	log.Warn("", zap.Uint64("level0mask", level0Mask))

	i.Table.Level0Mask = int(level0Mask)

	// MPH level1 mask
	level1Mask := bytesToU64(buf[:8])
	buf = buf[8:]
	log.Warn("", zap.Uint64("level1mask", level0Mask))

	i.Table.Level1Mask = int(level1Mask)

	// MPH level0 len
	level0Len := bytesToU64(buf[:8])
	buf = buf[8:]
	log.Warn("", zap.Uint64("level0Len", level0Len))

	i.Table.Level0 = make([]uint32, level0Len)

	// MPH level0[i]
	ii := uint64(0)
	for  {
		i.Table.Level0 = append(i.Table.Level0, bytesToU32(buf[:8]))
		log.Warn("level0", zap.Uint64("idx", ii), zap.Uint32("value", i.Table.Level0[ii]))
		buf = buf[4:]
		ii += 1
		if ii >= level0Len {
			break
		}
	}

	// MPH level0 len
	level1Len := bytesToU64(buf[:8])
	buf = buf[8:]
	log.Warn("", zap.Uint64("level1Len", level1Len))

	i.Table.Level1 = make([]uint32, level1Len)

	// MPH level1[i]
	ii = uint64(0)
	for  {
		i.Table.Level1 = append(i.Table.Level1, bytesToU32(buf[:8]))
		log.Warn("level1", zap.Uint64("idx", ii), zap.Uint32("value", i.Table.Level1[ii]))
		buf = buf[4:]
		ii += 1
		if ii >= level0Len {
			break
		}
	}

	// MPH entries[i]
	ii = uint64(0)
	for  {
		blkIdx := binary.LittleEndian.Uint16(buf)
		pos :=  uint8(buf[2])

		log.Warn("entry", zap.Uint64("idx", ii), zap.Uint16("blkIdx", blkIdx), zap.Uint8("pos", pos))

		i.Entries = append(i.Entries, entryPosition{blockIdx:  blkIdx, offset: pos})

		buf = buf[3:]
		ii += 1
		if ii >= entryCnt {
			break
		}
	}
}

func (i *MphIndex) lookup(key []byte) (uint32, uint8) {
	log.Warn("END mph lookup")
	defer func() {
		log.Warn("END mph lookup")
	}()
	idx := i.Table.Lookup(*(*string)(unsafe.Pointer(&key)))
	e := i.Entries[idx]
	return uint32(e.blockIdx), e.offset
}
