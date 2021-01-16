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
	key string
}

type MphIndex struct {
	Table *mph.Table
	Entries   []entryPosition
}

func buildMphIndex(hashEntries []MphEntry) []byte {
	//log.Warn("START TO BUILD MPH HASH. ", zap.Int("entryCnt", len(hashEntries)))
	if len(hashEntries) == 0 {
		return u32ToBytes(0)
	}

	idx := MphIndex{}

	keys := make([]string,0)
	entries := make([]entryPosition,0)

	for _, e := range hashEntries {
		keys = append(keys, e.key)
		entries = append(entries, e.entryPosition)
	}

	idx.Entries = entries
	//log.Warn("START idx.Table.Build")
	// for i, k := range keys {
	// 	log.Warn("ky", zap.Int("index", i), zap.String("value", string(k)))
	// }
	idx.Table = mph.Build(keys)
	//log.Warn("END idx.Table.Build")

	sz := uint64(len(hashEntries))

	// totalLen(8) + entryCnt(8) + Level0Mask(8) + Level1Mask(8) + Level0Size(8) +Level0(var) + Level1Size(8) +Level1(var) + 3*HashEntries
	var totalLen = uint64(8 + 8 + 8 + 8 + 8 + len(idx.Table.Level0) * 4 + 8 + len(idx.Table.Level1) * 4 + 3*len(hashEntries))
	buf := make([]byte, totalLen)

	p := buf[0:]

	// Total Len
	copy(p, u64ToBytes(totalLen))
	log.Warn("", zap.Uint64("WRITE TOTALEN", totalLen))
	p = p[8:]

	// Entry Size
	copy(p, u64ToBytes(sz))
	log.Warn("", zap.Uint64("WRITE ENTRYCNT", sz))
	p = p[8:]

	// MPH Level0 mask
	copy(p, u64ToBytes(uint64(idx.Table.Level0Mask)))
	log.Warn("", zap.Uint64("WRITE level0Mask", uint64(idx.Table.Level0Mask)))
	p = p[8:]

	// MPH Level1 mask
	copy(p, u64ToBytes(uint64(idx.Table.Level1Mask)))
	log.Warn("", zap.Uint64("WRITE level1Mask", uint64(idx.Table.Level1Mask)))
	p = p[8:]

	// MPH Level0 veclen
	copy(p, u64ToBytes(uint64(len(idx.Table.Level0))))
	log.Warn("", zap.Uint64("WRITE level0len", uint64(len(idx.Table.Level0))))
	p = p[8:]

	// MPH level0[i]
	for i, v := range idx.Table.Level0 {
		copy(p, u32ToBytes(v))
		log.Warn("", zap.Int("WRITE level0 index", i), zap.Uint32("value", v))
		p = p[4:]
	}

	// MPH Level1 veclen
	copy(p, u64ToBytes(uint64(len(idx.Table.Level1))))
	log.Warn("", zap.Uint64("WRITE level0len", uint64(len(idx.Table.Level1))))
	p = p[8:]

	// MPH level1[i]
	for i, v := range idx.Table.Level1 {
		copy(p, u32ToBytes(v))
		log.Warn("", zap.Int("WRITE level1 index", i), zap.Uint32("value", v))
		p = p[4:]
	}

	// Hash Entries
	for i, e := range hashEntries {
		binary.LittleEndian.PutUint16(p[:2], e.blockIdx)
		log.Warn("", zap.Int("WRITE entry index", i), zap.Uint16("blockIdx", e.blockIdx), zap.Uint8("offset", e.offset))
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
	log.Warn("", zap.Uint64("TOTALEN", totalLen))
	buf = buf[8:]

	// Entry cnt
	entryCnt := bytesToU64(buf[:8])
	log.Warn("", zap.Uint64("ENTRY CNT", entryCnt))
	buf = buf[8:]

	// MPH level0 mask
	level0Mask := bytesToU64(buf[:8])
	log.Warn("", zap.Uint64("level0mask", level0Mask))
	buf = buf[8:]

	i.Table.Level0Mask = int(level0Mask)

	// MPH level1 mask
	level1Mask := bytesToU64(buf[:8])
	log.Warn("", zap.Uint64("level1mask", level1Mask))
	buf = buf[8:]

	i.Table.Level1Mask = int(level1Mask)

	// MPH level0 len
	level0Len := bytesToU64(buf[:8])
	log.Warn("", zap.Uint64("level0Len", level0Len))
	buf = buf[8:]

	i.Table.Level0 = make([]uint32,0)

	// MPH level0[i]
	ii := uint64(0)
	for  {
		v := bytesToU32(buf[:4])
		buf = buf[4:]
		log.Warn("level0", zap.Uint64("idx", ii), zap.Uint32("value", v))
		i.Table.Level0 = append(i.Table.Level0, v)
		ii += 1
		if ii >= level0Len {
			break
		}
	}

	// MPH level1 len
	level1Len := bytesToU64(buf[:8])
	buf = buf[8:]
	log.Warn("", zap.Uint64("level1Len", level1Len))

	i.Table.Level1 = make([]uint32,0)

	// MPH level1[i]
	ii = uint64(0)
	for  {
		v := bytesToU32(buf[:4])
		buf = buf[4:]
		i.Table.Level1 = append(i.Table.Level1, v)
		log.Warn("level1", zap.Uint64("idx", ii), zap.Uint32("value", v))
		ii += 1
		if ii >= level1Len {
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
	log.Warn("", zap.Uint16("INDEX", e.blockIdx), zap.Uint8("VALUE", e.offset))
	return uint32(e.blockIdx), e.offset
}
