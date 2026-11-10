// Copyright 2020 rateLimit Author(https://github.com/yudeguang/noGCMap). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/yudeguang/noGCMap.
package noGCMap

import (
	"bytes"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const (
	// intKeyLen int 键固定用 8 字节大端序保存。
	intKeyLen = 8
	// intPairHeaderLen 头部：1 字节删除标记 + 2 字节值长度。
	intPairHeaderLen = 3
)

// chunksViewInt 是某个分片存储区的不可变快照，用于不加锁地读取 key。
//
// 每个 chunk 的“已写入长度”用独立的原子变量记录（lens），视图与长度属于同一份快照，
// 因此不存在“视图与写入边界分两次发布、中间被压缩打断”的撕裂问题。
type chunksViewInt struct {
	chunks [][]byte
	lens   []*uint64
}

// slowKeysViewInt 是 slow 表 key 的历史日志快照（只追加）。
type slowKeysViewInt struct {
	keys []int
}

// NoGcMapInt 以 int 为键的、面向大规模数据、几乎不产生 GC 压力的并发安全 Map。
type NoGcMapInt struct {
	// chunkSize 本实例的 chunk 大小，构建实例后不再改变。
	chunkSize int
	// needDeletePercent 本实例的回收阈值。
	needDeletePercent int
	// lock 保护 slow 表与 slow key 日志。
	lock sync.RWMutex
	// slow 存放键值对超长、无法放进分片的数据（这部分是少数）。
	slow map[int][]byte
	// slowKeys slow 表的 key 历史日志，只追加，供不加锁的 KeyToSliceUnSafe 使用。
	slowKeys atomic.Value
	// buckets 数据分片。
	buckets [bucketsNum]bucketInt
}

type bucketInt struct {
	lock sync.RWMutex
	// dataBeginPos 下一个键值对在本分片中的全局线性偏移（chunkIndex*chunkSize + chunk 内偏移）。
	dataBeginPos uint
	// compactVersion 压缩版本号：压缩期间为奇数，稳定期为偶数（仅用于观测与测试）。
	compactVersion uint64
	// curChunkIndex 最新写入的数据在 chunks 中的下标，始终等于 len(chunks)-1。
	curChunkIndex int
	// deleteItemNum 本分片中被标记删除的键值对个数（仅用于观察）。
	deleteItemNum int
	// deleteBytes 本分片中被标记删除的键值对占用的字节数，用于判断是否需要回收。
	deleteBytes int
	// slowItemNum 本分片中落在 slow 表里的 key 个数。
	slowItemNum int
	// index 值所在 chunk 的全局线性偏移。int 键本身不冲突，直接以 key 为索引。
	index map[int]uint
	// chunks 键值对真实存储位置，每个 chunk 容量固定为 chunkSize。
	chunks [][]byte
	// lens 每个 chunk 的已写入长度（原子），供不加锁的 key 扫描使用。
	lens []*uint64
	// view 供无锁扫描使用的只读快照，只在 chunk 数量变化或压缩后发布。
	view atomic.Value
}

// NewInt 创建 NoGcMapInt。config 可选，非法配置会被忽略并回退到默认值。
// 需要严格校验配置时请使用 NewIntWithError。
func NewInt(config ...Config) *NoGcMapInt {
	n, err := NewIntWithError(config...)
	if err != nil {
		n, _ = NewIntWithError()
	}
	return n
}

// NewIntWithError 创建 NoGcMapInt，并对 config 做严格校验。
func NewIntWithError(config ...Config) (*NoGcMapInt, error) {
	chunkSize, needDeletePercent, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	n := &NoGcMapInt{
		chunkSize:         chunkSize,
		needDeletePercent: needDeletePercent,
		slow:              make(map[int][]byte),
	}
	for i := range n.buckets {
		n.buckets[i].index = make(map[int]uint)
	}
	return n, nil
}

// ChunkSize 返回本实例实际生效的 chunk 大小。
func (n *NoGcMapInt) ChunkSize() int {
	return n.chunkSize
}

// NeedDeletePercent 返回本实例实际生效的回收阈值。
func (n *NoGcMapInt) NeedDeletePercent() int {
	return n.needDeletePercent
}

// bucketIndexOf 计算 int 键所属分片。
func bucketIndexOf(k int) int {
	idx := k % bucketsNum
	if idx < 0 {
		idx = -idx
	}
	return idx
}

// Get 取出数据，返回值为数据副本，调用方可以随意修改。
// 需要零分配的读取请使用 GetTo。
func (n *NoGcMapInt) Get(k int) (v []byte, exist bool) {
	return n.GetTo(nil, k)
}

// GetTo 取出数据并追加到 dst（dst[:0] 之后复用），避免每次读取都分配内存。
func (n *NoGcMapInt) GetTo(dst []byte, k int) (v []byte, exist bool) {
	b := &n.buckets[bucketIndexOf(k)]
	b.lock.RLock()
	defer b.lock.RUnlock()
	if dataBeginPos, ok := b.index[k]; ok {
		if v, exist = n.read(b, k, dataBeginPos, dst); exist {
			return v, true
		}
	}
	// 分片内没找到，再从 slow 表查找
	if b.slowItemNum == 0 {
		return nil, false
	}
	n.lock.RLock()
	defer n.lock.RUnlock()
	val, exist := n.slow[k]
	if !exist {
		return nil, false
	}
	return append(dst, val...), true
}

// GetString 取出数据，以 string 的方式返回。
func (n *NoGcMapInt) GetString(k int) (v string, exist bool) {
	val, exist := n.Get(k)
	if exist {
		return string(val), true
	}
	return "", false
}

// Set 增加或更新数据。函数内部会复制 v 的内容，调用方可以复用传入的切片。
func (n *NoGcMapInt) Set(k int, v []byte) {
	// 超长值放到 slow 表
	if len(v)+intKeyLen+intPairHeaderLen > n.chunkSize || len(v) > maxKVLen {
		n.setSlow(k, v)
		return
	}
	b := &n.buckets[bucketIndexOf(k)]
	b.lock.Lock()
	if dataBeginPos, ok := b.index[k]; ok {
		if vv, exist := n.lookup(b, k, dataBeginPos); exist {
			if bytes.Equal(vv, v) {
				b.lock.Unlock()
				return
			}
			n.markAsDelete(b, k, dataBeginPos)
		}
	}
	n.write(b, k, v)
	// 该 key 之前可能在 slow 表中，写入分片后必须清掉，保证同一个 key 只有一个落点
	if b.slowItemNum > 0 {
		n.dropSlow(b, k)
	}
	n.maybeCompact(b)
	b.lock.Unlock()
}

// SetString 增加或更新数据，以 string 的形式。
func (n *NoGcMapInt) SetString(k int, v string) {
	n.Set(k, []byte(v))
}

// Delete 删除数据。无论 key 落在分片还是 slow 表，都会被删除。
func (n *NoGcMapInt) Delete(k int) {
	b := &n.buckets[bucketIndexOf(k)]
	b.lock.Lock()
	defer b.lock.Unlock()
	if dataBeginPos, ok := b.index[k]; ok {
		if n.markAsDelete(b, k, dataBeginPos) {
			delete(b.index, k)
		}
	}
	if b.slowItemNum > 0 {
		n.dropSlow(b, k)
	}
	n.maybeCompact(b)
}

// Len 返回键值对数量。并发写入时该值是近似值（不做全局快照）。
func (n *NoGcMapInt) Len() int {
	num := 0
	for i := range n.buckets {
		b := &n.buckets[i]
		b.lock.RLock()
		num += len(b.index)
		b.lock.RUnlock()
	}
	n.lock.RLock()
	num += len(n.slow)
	n.lock.RUnlock()
	return num
}

// KeyToSliceUnSafe 在不加任何 map 锁的前提下，把本 Map 中出现过的所有 key 追加到 dst 并返回。
//
// 语义约定与 NoGcMapAny.KeyToSliceUnSafe 一致：
// 结果只会“多”不会“少”；可能包含已删除的 key 与重复 key，使用前请自行 Get 判断是否仍然存在。
func (n *NoGcMapInt) KeyToSliceUnSafe(dst []int) []int {
	for i := range n.buckets {
		dst = n.appendKeysOfBucket(&n.buckets[i], dst)
	}
	if v, ok := n.slowKeys.Load().(*slowKeysViewInt); ok && v != nil {
		dst = append(dst, v.keys...)
	}
	return dst
}

// keyToSliceUnSafe 是 KeyToSliceUnSafe 的包内别名。
func (n *NoGcMapInt) keyToSliceUnSafe(dst []int) []int {
	return n.KeyToSliceUnSafe(dst)
}

// appendKeysOfBucket 无锁读取单个分片上的全部 key。
//
// 只读取一次分片发布的只读视图：视图把每个 chunk 的“已写入长度”与其数据绑在同一份快照里，
// 因此不需要版本重试，也不会出现“旧视图 + 新边界”这种跨版本错配导致的漏 key。
func (n *NoGcMapInt) appendKeysOfBucket(b *bucketInt, dst []int) []int {
	view, _ := b.view.Load().(*chunksViewInt)
	if view == nil || len(view.chunks) == 0 {
		return dst
	}
	return parseChunkKeysInt(view, n.chunkSize, dst)
}

// parseChunkKeysInt 解析 chunk 视图中的全部 key。
// 每个 chunk 只解析到它自己的已写入长度，长度与数据属于同一份快照，既不会漏 key，也不会读到未写区域。
func parseChunkKeysInt(view *chunksViewInt, chunkSize int, dst []int) []int {
	zeroAdded := false
	for i, buf := range view.chunks {
		if i >= len(view.lens) {
			break
		}
		written := int(atomic.LoadUint64(view.lens[i]))
		if written <= 0 {
			continue
		}
		if written > cap(buf) {
			written = cap(buf)
		}
		if written > chunkSize {
			written = chunkSize
		}
		c := buf[:written]
		offset := 0
		for offset+intPairHeaderLen+intKeyLen <= len(c) {
			// 同样不读“删除标记”字节（c[offset]）：它与 markAsDelete 的就地写入之间没有
			// happens-before 边，而标记对输出没有影响，不读即可彻底消除数据竞争。
			valLen := int(binary.BigEndian.Uint16(c[offset+1 : offset+intPairHeaderLen]))
			pairLen := intPairHeaderLen + intKeyLen + valLen
			if offset+pairLen > len(c) {
				break
			}
			key := int(binary.BigEndian.Uint64(c[offset+intPairHeaderLen : offset+intPairHeaderLen+intKeyLen]))
			if key == 0 && valLen == 0 {
				// 0 号键 + 空值 与未写区域的字节一致，只收一次，避免尾部零区刷出大量重复项
				if !zeroAdded {
					zeroAdded = true
					dst = append(dst, 0)
				}
			} else {
				dst = append(dst, key)
			}
			offset += pairLen
		}
	}
	return dst
}

// setSlow 把键值对写入 slow 表，并清理分片中可能存在的同一 key 的旧记录。
func (n *NoGcMapInt) setSlow(k int, v []byte) {
	b := &n.buckets[bucketIndexOf(k)]
	b.lock.Lock()
	defer b.lock.Unlock()
	if dataBeginPos, ok := b.index[k]; ok {
		if _, exist := n.lookup(b, k, dataBeginPos); exist {
			n.markAsDelete(b, k, dataBeginPos)
			delete(b.index, k)
			n.maybeCompact(b)
		}
	}
	n.lock.Lock()
	_, existed := n.slow[k]
	if !existed {
		b.slowItemNum++
	}
	// 复制值，避免保存调用方的切片导致内容被后续复用篡改
	n.slow[k] = append([]byte(nil), v...)
	if !existed {
		n.appendSlowKeyLocked(k)
	}
	n.lock.Unlock()
}

// appendSlowKeyLocked 把 slow 表的 key 追加进只读日志（调用方必须持有 n.lock 写锁）。
func (n *NoGcMapInt) appendSlowKeyLocked(k int) {
	cur, _ := n.slowKeys.Load().(*slowKeysViewInt)
	if cur == nil {
		n.slowKeys.Store(&slowKeysViewInt{keys: []int{k}})
		return
	}
	if len(cur.keys) > 2*len(n.slow)+1024 {
		// 日志膨胀：按当前 slow 表重建，避免无限增长；重建后仍要补上本次新增的 key
		keys := make([]int, 0, len(n.slow)+1)
		added := false
		for key := range n.slow {
			keys = append(keys, key)
			if key == k {
				added = true
			}
		}
		if !added {
			keys = append(keys, k)
		}
		n.slowKeys.Store(&slowKeysViewInt{keys: keys})
		return
	}
	keys := append(cur.keys, k)
	n.slowKeys.Store(&slowKeysViewInt{keys: keys})
}

// dropSlow 删除 slow 表中的 key（调用方必须已持有该分片的写锁）。
func (n *NoGcMapInt) dropSlow(b *bucketInt, k int) {
	n.lock.Lock()
	if _, ok := n.slow[k]; ok {
		delete(n.slow, k)
		b.slowItemNum--
	}
	n.lock.Unlock()
}

// markAsDelete 把键值对头部标记为已删除，并增加删除计数。返回 key 是否匹配。
func (n *NoGcMapInt) markAsDelete(b *bucketInt, k int, dataBeginPos uint) bool {
	chunkIndex := int(dataBeginPos) / n.chunkSize
	offset := int(dataBeginPos) % n.chunkSize
	if chunkIndex < 0 || chunkIndex >= len(b.chunks) {
		return false
	}
	chunk := b.chunks[chunkIndex]
	if offset+intPairHeaderLen+intKeyLen > len(chunk) {
		return false
	}
	if binary.BigEndian.Uint64(chunk[offset+intPairHeaderLen:offset+intPairHeaderLen+intKeyLen]) != uint64(int64(k)) {
		return false
	}
	chunk[offset] = 1
	b.deleteItemNum++
	b.deleteBytes += intPairHeaderLen + intKeyLen + int(binary.BigEndian.Uint16(chunk[offset+1:offset+intPairHeaderLen]))
	return true
}

// lookup 在分片内定位键值对，返回的是内部存储的切片。
// 只能在持有分片锁期间使用，调用方不得持有或修改返回的切片。
func (n *NoGcMapInt) lookup(b *bucketInt, k int, dataBeginPos uint) ([]byte, bool) {
	chunkIndex := int(dataBeginPos) / n.chunkSize
	offset := int(dataBeginPos) % n.chunkSize
	if chunkIndex < 0 || chunkIndex >= len(b.chunks) {
		return nil, false
	}
	chunk := b.chunks[chunkIndex]
	if offset+intPairHeaderLen+intKeyLen > len(chunk) {
		return nil, false
	}
	valLen := int(binary.BigEndian.Uint16(chunk[offset+1 : offset+intPairHeaderLen]))
	if binary.BigEndian.Uint64(chunk[offset+intPairHeaderLen:offset+intPairHeaderLen+intKeyLen]) != uint64(int64(k)) {
		return nil, false
	}
	valBegin := offset + intPairHeaderLen + intKeyLen
	if valBegin+valLen > len(chunk) {
		return nil, false
	}
	return chunk[valBegin : valBegin+valLen], true
}

// read 读取键值对，命中时把值追加到 dst。返回的切片不会与内部存储共享底层数组。
func (n *NoGcMapInt) read(b *bucketInt, k int, dataBeginPos uint, dst []byte) ([]byte, bool) {
	val, ok := n.lookup(b, k, dataBeginPos)
	if !ok {
		return nil, false
	}
	return append(dst, val...), true
}

// ensureWriteSpace 保证当前 chunk 能容纳 pairLen 字节；不够则新建 chunk。
// 返回是否新建了 chunk（新建后需要重新发布只读视图）。
func (n *NoGcMapInt) ensureWriteSpace(b *bucketInt, pairLen int) bool {
	if len(b.chunks) == 0 {
		b.curChunkIndex = 0
		b.dataBeginPos = 0
		b.chunks = append(b.chunks, make([]byte, 0, n.chunkSize))
		b.lens = append(b.lens, new(uint64))
		return true
	}
	if cap(b.chunks[b.curChunkIndex])-len(b.chunks[b.curChunkIndex]) < pairLen {
		b.curChunkIndex++
		b.dataBeginPos = uint(n.chunkSize * b.curChunkIndex)
		b.chunks = append(b.chunks, make([]byte, 0, n.chunkSize))
		b.lens = append(b.lens, new(uint64))
		return true
	}
	return false
}

// publishLocked 发布分片的只读 chunks 视图（调用方必须持有分片写锁）。
func (n *NoGcMapInt) publishLocked(b *bucketInt) {
	chunks := make([][]byte, len(b.chunks))
	copy(chunks, b.chunks)
	lens := make([]*uint64, len(b.lens))
	copy(lens, b.lens)
	b.view.Store(&chunksViewInt{chunks: chunks, lens: lens})
}

// write 追加一条键值对，并把 index 指向它的位置。
func (n *NoGcMapInt) write(b *bucketInt, k int, v []byte) {
	pairLen := intPairHeaderLen + intKeyLen + len(v)
	if n.ensureWriteSpace(b, pairLen) {
		n.publishLocked(b)
	}
	chunk := b.chunks[b.curChunkIndex]
	chunk = append(chunk, 0, byte(len(v)>>8), byte(len(v)))
	var keyBuf [intKeyLen]byte
	binary.BigEndian.PutUint64(keyBuf[:], uint64(int64(k)))
	chunk = append(chunk, keyBuf[:]...)
	chunk = append(chunk, v...)
	b.chunks[b.curChunkIndex] = chunk
	// 发布该 chunk 的已写入长度：先写数据、后发布长度，扫描端读到长度即能安全读到对应的数据
	atomic.StoreUint64(b.lens[b.curChunkIndex], uint64(len(chunk)))
	b.index[k] = b.dataBeginPos
	b.dataBeginPos += uint(pairLen)
}

// maybeCompact 在已删除记录占比超过阈值时，重排当前分片、回收闲置记录。
// 调用方必须持有该分片的写锁。
func (n *NoGcMapInt) maybeCompact(b *bucketInt) {
	if b.deleteBytes < n.compactMinBytes() {
		return
	}
	if b.dataBeginPos == 0 {
		return
	}
	if b.deleteBytes*100 < int(b.dataBeginPos)*n.needDeletePercent {
		return
	}
	n.compact(b)
}

// compactMinBytes 返回触发回收所需的最小浪费字节数。
func (n *NoGcMapInt) compactMinBytes() int {
	// 默认浪费半个 chunk 才压缩：压缩采用写时复制，频率过高会带来明显的额外分配
	minBytes := n.chunkSize / 2
	if minBytes < minCompactBytesFloor {
		minBytes = minCompactBytesFloor
	}
	if minBytes > minCompactBytesCeil {
		minBytes = minCompactBytesCeil
	}
	return minBytes
}

// compact 重排当前分片：只保留存活记录，写入全新的 chunk，并重建索引。
//
// 与 NoGcMapAny 一致，采用“写时复制”：旧 chunk 不被改写，供无锁 key 扫描安全读取。
// 调用方必须持有该分片的写锁。
func (n *NoGcMapInt) compact(b *bucketInt) {
	atomic.AddUint64(&b.compactVersion, 1)       // 进入压缩（奇数）
	defer atomic.AddUint64(&b.compactVersion, 1) // 退出压缩（偶数）
	if len(b.chunks) == 0 {
		return
	}
	newChunks := make([][]byte, 0, len(b.chunks))
	newLens := make([]*uint64, 0, len(b.lens))
	newIndex := make(map[int]uint, len(b.index))
	pos := uint(0)
	chunkIndex := 0
	for _, chunk := range b.chunks {
		if len(chunk) == 0 {
			continue
		}
		for offset := 0; offset+intPairHeaderLen+intKeyLen <= len(chunk); {
			valLen := int(binary.BigEndian.Uint16(chunk[offset+1 : offset+intPairHeaderLen]))
			pairLen := intPairHeaderLen + intKeyLen + valLen
			if offset+pairLen > len(chunk) {
				break
			}
			if chunk[offset] == 0 {
				pair := chunk[offset : offset+pairLen]
				k := int(binary.BigEndian.Uint64(pair[intPairHeaderLen : intPairHeaderLen+intKeyLen]))
				if len(newChunks) == 0 {
					newChunks = append(newChunks, make([]byte, 0, n.chunkSize))
					newLens = append(newLens, new(uint64))
					chunkIndex = 0
					pos = 0
				}
				if cap(newChunks[chunkIndex])-len(newChunks[chunkIndex]) < pairLen {
					chunkIndex++
					pos = uint(n.chunkSize * chunkIndex)
					newChunks = append(newChunks, make([]byte, 0, n.chunkSize))
					newLens = append(newLens, new(uint64))
				}
				newChunks[chunkIndex] = append(newChunks[chunkIndex], pair...)
				newIndex[k] = pos
				pos += uint(pairLen)
			}
			offset += pairLen
		}
	}
	// 先写入新 chunk 的数据与长度，再整体发布视图：旧视图里的 chunk 长度不再变化，可安全读完
	for i := range newChunks {
		atomic.StoreUint64(newLens[i], uint64(len(newChunks[i])))
	}
	b.chunks = newChunks
	b.lens = newLens
	b.index = newIndex
	b.curChunkIndex = chunkIndex
	b.dataBeginPos = pos
	b.deleteItemNum = 0
	b.deleteBytes = 0
	n.publishLocked(b)
}
