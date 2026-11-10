// Copyright 2020 rateLimit Author(https://github.com/yudeguang/noGCMap). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/yudeguang/noGCMap.
package noGCMap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/maphash"
	"sync"
	"sync/atomic"
)

const (
	// bucketsNum 数据分片个数，分片可以显著降低锁竞争。
	bucketsNum = 512
	// defaultChunkSize 默认的 chunk 大小：64K。
	defaultChunkSize = 64 * 1024
	// defaultNeedDeletePercent 默认的回收阈值：已删除记录占该分片全部记录（存活 + 已删除）的 20%。
	defaultNeedDeletePercent = 20
	// maxKVLen 单个键或值允许的最大长度，长度只用 2 字节记录。
	maxKVLen = 65535
	// kvPairHeaderLen 键值对头部长度：1 字节删除标记 + 2 字节键长 + 2 字节值长。
	kvPairHeaderLen = 5
	// minCompactBytesFloor / minCompactBytesCeil 触发回收前，本分片至少被浪费的字节数下限与上限。
	// 下限避免少量删除就做全分片重排，上限避免 chunk 配置很大时长期不回收。
	minCompactBytesFloor = 8 * 1024
	minCompactBytesCeil  = 1024 * 1024
)

var (
	// ErrNeedDeletePercentNeedDeketeKVPairPersent 取值非法（合法范围 1~100）。
	ErrNeedDeletePercentNeedDeketeKVPairPersent = errors.New("noGCMap: NeedDeketeKVPairPersent 必须介于 1 和 100 之间")
	// ErrChunkSizeMultiples MultiplesOf64KForchunkSize 取值非法（合法范围 1~1000）。
	ErrChunkSizeMultiples = errors.New("noGCMap: MultiplesOf64KForchunkSize 必须介于 1 和 1000 之间")
)

// Config 实例级配置。配置只在创建实例时生效，不再影响其它已存在的实例。
type Config struct {
	// NeedDeketeKVPairPersent 已删除记录占该分片全部记录（存活 + 已删除）的百分比达到该值时，
	// 触发一轮真正的删除与内存回收。取值 1~100，默认 20。
	// 取 100 表示只有分片内全部记录都被删除后才回收。
	NeedDeketeKVPairPersent int
	// MultiplesOf64KForchunkSize 用于定义 chunkSize，
	// 最终 chunkSize = MultiplesOf64KForchunkSize * 64 * 1024。
	// 取值 1~1000（1 => 64K，1000 => 64M），默认 1。
	MultiplesOf64KForchunkSize int
}

// chunksView 是某个分片存储区的不可变快照，用于不加锁地读取 key。
//
// 每个 chunk 的“已写入长度”用独立的原子变量记录（lens），所以扫描只需要一次视图读取：
// 视图里的 chunk 与它的长度天然属于同一份快照，不存在“视图与写入边界分两次发布、
// 中间被压缩打断”的撕裂问题。写入只会向 chunk 的末尾追加，压缩则整体替换成新的 chunk。
type chunksView struct {
	chunks [][]byte
	lens   []*uint64
}

// slowKeysView 是 slow 表 key 的历史日志快照（只追加）。
type slowKeysView struct {
	keys []string
}

// NoGcMapAny 以 []byte 为键的、面向大规模数据、几乎不产生 GC 压力的并发安全 Map。
type NoGcMapAny struct {
	// seed 用于 key 的哈希计算。每个实例独立随机，既能避免不同实例间哈希分布相关，
	// 也天然具备抗哈希碰撞攻击（HashDoS）的能力。
	seed maphash.Seed
	// chunkSize 本实例的 chunk 大小，构建实例后不再改变。
	chunkSize int
	// needDeletePercent 本实例的回收阈值。
	needDeletePercent int
	// lock 保护 slow 表与 slow key 日志。
	lock sync.RWMutex
	// slow 存放哈希冲突的第 2 个及以后的 key，以及键或值超长的数据（这部分是少数）。
	slow map[string][]byte
	// slowKeys slow 表的 key 历史日志，只追加，供不加锁的 KeyToSliceUnSafe 使用。
	slowKeys atomic.Value
	// buckets 数据分片。
	buckets [bucketsNum]bucket
}

type bucket struct {
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
	// 用于让绝大多数写操作跳过全局锁。
	slowItemNum int
	// index 值所在 chunk 的全局线性偏移。key 为哈希值；哈希冲突时只有第一个 key 会出现在这里。
	index map[uint64]uint
	// chunks 键值对真实存储位置，每个 chunk 容量固定为 chunkSize。
	chunks [][]byte
	// lens 每个 chunk 的已写入长度（原子），供不加锁的 key 扫描使用。
	lens []*uint64
	// view 供无锁扫描使用的只读快照，只在 chunk 数量变化或压缩后发布。
	view atomic.Value
}

// New 创建 NoGcMapAny。config 可选，非法配置会被忽略并回退到默认值。
// 需要严格校验配置时请使用 NewWithError。
func New(config ...Config) *NoGcMapAny {
	n, err := NewWithError(config...)
	if err != nil {
		// 不回退到 panic：保持历史行为，非法配置按默认值创建。
		n, _ = NewWithError()
	}
	return n
}

// NewWithError 创建 NoGcMapAny，并对 config 做严格校验。
func NewWithError(config ...Config) (*NoGcMapAny, error) {
	chunkSize, needDeletePercent, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	n := &NoGcMapAny{
		seed:              maphash.MakeSeed(),
		chunkSize:         chunkSize,
		needDeletePercent: needDeletePercent,
		slow:              make(map[string][]byte),
	}
	for i := range n.buckets {
		n.buckets[i].index = make(map[uint64]uint)
	}
	return n, nil
}

// normalizeConfig 校验并归一化配置，供 NoGcMapAny / NoGcMapInt 共用。
func normalizeConfig(config []Config) (chunkSize int, needDeletePercent int, err error) {
	chunkSize = defaultChunkSize
	needDeletePercent = defaultNeedDeletePercent
	if len(config) == 0 {
		return chunkSize, needDeletePercent, nil
	}
	c := config[0]
	if c.NeedDeketeKVPairPersent != 0 {
		if c.NeedDeketeKVPairPersent < 1 || c.NeedDeketeKVPairPersent > 100 {
			return 0, 0, ErrNeedDeletePercentNeedDeketeKVPairPersent
		}
		needDeletePercent = c.NeedDeketeKVPairPersent
	}
	if c.MultiplesOf64KForchunkSize != 0 {
		if c.MultiplesOf64KForchunkSize < 1 || c.MultiplesOf64KForchunkSize > 1000 {
			return 0, 0, ErrChunkSizeMultiples
		}
		chunkSize = c.MultiplesOf64KForchunkSize * 64 * 1024
	}
	return chunkSize, needDeletePercent, nil
}

// ChunkSize 返回本实例实际生效的 chunk 大小。
func (n *NoGcMapAny) ChunkSize() int {
	return n.chunkSize
}

// NeedDeletePercent 返回本实例实际生效的回收阈值。
func (n *NoGcMapAny) NeedDeletePercent() int {
	return n.needDeletePercent
}

// Get 取出数据，返回值为数据副本，调用方可以随意修改。
// 需要零分配的读取请使用 GetTo。
func (n *NoGcMapAny) Get(k []byte) (v []byte, exist bool) {
	return n.GetTo(nil, k)
}

// GetTo 取出数据并追加到 dst（dst[:0] 之后复用），避免每次读取都分配内存。
// 返回的切片底层数组可能是 dst，也可能是内部新建的切片；无论哪种，都不会与 Map 内部存储共享。
func (n *NoGcMapAny) GetTo(dst []byte, k []byte) (v []byte, exist bool) {
	h := maphash.Bytes(n.seed, k)
	b := &n.buckets[h%bucketsNum]
	b.lock.RLock()
	defer b.lock.RUnlock()
	if dataBeginPos, ok := b.index[h]; ok {
		if v, exist = n.read(b, k, dataBeginPos, dst); exist {
			return v, true
		}
	}
	// 分片内没找到，再从可能存在哈希冲突或超长键值的 slow 表查找。
	if b.slowItemNum == 0 {
		return nil, false
	}
	n.lock.RLock()
	defer n.lock.RUnlock()
	val, exist := n.slow[string(k)]
	if !exist {
		return nil, false
	}
	return append(dst, val...), true
}

// GetString 取出数据，以 string 的方式返回。
func (n *NoGcMapAny) GetString(k string) (v string, exist bool) {
	val, exist := n.Get([]byte(k))
	if exist {
		return string(val), true
	}
	return "", false
}

// Set 增加或更新数据。函数内部会复制 k、v 的内容，调用方可以复用传入的切片。
func (n *NoGcMapAny) Set(k, v []byte) {
	h := maphash.Bytes(n.seed, k)
	// 超长键值对与哈希冲突的第二条记录都放到 slow 表。
	if len(k)+len(v)+kvPairHeaderLen > n.chunkSize || len(k) > maxKVLen || len(v) > maxKVLen {
		n.setSlow(h, k, v)
		return
	}
	b := &n.buckets[h%bucketsNum]
	b.lock.Lock()
	if dataBeginPos, ok := b.index[h]; ok {
		vv, exist := n.lookup(b, k, dataBeginPos)
		if exist {
			if bytes.Equal(vv, v) {
				// 值没有变化，无需写入
				b.lock.Unlock()
				return
			}
			// 值发生变化：标记旧记录删除，随后追加一条新记录
			n.markAsDelete(b, k, dataBeginPos)
		} else {
			// 哈希冲突：让当前 key 走 slow 表，避免覆盖另一方在 index 中的位置
			b.lock.Unlock()
			n.setSlow(h, k, v)
			return
		}
	}
	n.write(b, h, k, v)
	// 该 key 之前可能在 slow 表中，写入分片后必须清掉，保证同一个 key 只有一个落点
	if b.slowItemNum > 0 {
		n.dropSlow(b, k)
	}
	n.maybeCompact(b)
	b.lock.Unlock()
}

// SetString 增加或更新数据，以 string 的形式。
func (n *NoGcMapAny) SetString(k, v string) {
	n.Set([]byte(k), []byte(v))
}

// Delete 删除数据。无论 key 落在分片还是 slow 表，都会被删除。
func (n *NoGcMapAny) Delete(k []byte) {
	h := maphash.Bytes(n.seed, k)
	b := &n.buckets[h%bucketsNum]
	b.lock.Lock()
	defer b.lock.Unlock()
	if dataBeginPos, ok := b.index[h]; ok {
		if n.markAsDelete(b, k, dataBeginPos) {
			delete(b.index, h)
		}
	}
	if b.slowItemNum > 0 {
		n.dropSlow(b, k)
	}
	n.maybeCompact(b)
}

// DeleteString 删除数据，以 string 的形式。
func (n *NoGcMapAny) DeleteString(k string) {
	n.Delete([]byte(k))
}

// Len 返回键值对数量。并发写入时该值是近似值（不做全局快照）。
func (n *NoGcMapAny) Len() int {
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
// 语义约定：
//  1. 返回结果只会“多”，不会“少”：调用时刻已经存在的 key 一定在其中；
//  2. 结果里可能包含已删除的 key、同一 key 的历史版本（同一个 key 可能出现多次）；
//     使用每个 key 之前，请自行 Get 判断它是否仍然存在，需要唯一 key 时请自行去重；
//  3. 扫描期间新写入的 key 可能不出现，也可能出现，不做保证。
//
// 实现上不获取分片锁与全局锁，只读取一次分片对外发布的只读视图；
// 视图里每个 chunk 都带着自己的已写入长度，长度与数据同属一份快照，
// 因此不需要重试，也不会读到被就地覆写的数据或漏掉 key。
func (n *NoGcMapAny) KeyToSliceUnSafe(dst [][]byte) [][]byte {
	for i := range n.buckets {
		dst = n.appendKeysOfBucket(&n.buckets[i], dst)
	}
	if v, ok := n.slowKeys.Load().(*slowKeysView); ok && v != nil {
		for _, k := range v.keys {
			dst = append(dst, []byte(k))
		}
	}
	return dst
}

// keyToSliceUnSafe 是 KeyToSliceUnSafe 的包内别名。
func (n *NoGcMapAny) keyToSliceUnSafe(dst [][]byte) [][]byte {
	return n.KeyToSliceUnSafe(dst)
}

// appendKeysOfBucket 无锁读取单个分片上的全部 key。
//
// 只读取一次分片发布的只读视图：视图把每个 chunk 的“已写入长度”与其数据绑在同一份快照里，
// 因此不需要版本重试，也不会出现“旧视图 + 新边界”这种跨版本错配导致的漏 key。
func (n *NoGcMapAny) appendKeysOfBucket(b *bucket, dst [][]byte) [][]byte {
	view, _ := b.view.Load().(*chunksView)
	if view == nil || len(view.chunks) == 0 {
		return dst
	}
	return parseChunkKeys(view, n.chunkSize, dst)
}

// parseChunkKeys 解析 chunk 视图中的全部 key。
// 每个 chunk 只解析到它自己的已写入长度，长度与数据属于同一份快照，既不会漏 key，也不会读到未写区域。
func parseChunkKeys(view *chunksView, chunkSize int, dst [][]byte) [][]byte {
	emptyAdded := false
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
		for offset+kvPairHeaderLen <= len(c) {
			// 注意：这里刻意不读“删除标记”字节（c[offset]）。
			// 标记由 markAsDelete 在持锁时就地改写，而无锁扫描与它之间没有 happens-before 边；
			// 标记对输出没有影响（删除后的 key 本来就允许出现在结果里，属于“多”），
			// 不读它就彻底消除了这条数据竞争。
			keyLen := int(binary.BigEndian.Uint16(c[offset+1 : offset+3]))
			valLen := int(binary.BigEndian.Uint16(c[offset+3 : offset+5]))
			pairLen := kvPairHeaderLen + keyLen + valLen
			if offset+pairLen > len(c) {
				break
			}
			if keyLen == 0 {
				// 空 key 与未写区域的字节一致，只收一次，保证不会因为尾部零区刷出大量重复项
				if !emptyAdded {
					emptyAdded = true
					dst = append(dst, []byte{})
				}
			} else {
				key := c[offset+kvPairHeaderLen : offset+kvPairHeaderLen+keyLen]
				dst = append(dst, append([]byte(nil), key...))
			}
			offset += pairLen
		}
	}
	return dst
}

// setSlow 把键值对写入 slow 表，并清理分片中可能存在的同一 key 的旧记录。
// 调用顺序保证：先分片锁，后全局锁，与其它路径一致。
func (n *NoGcMapAny) setSlow(h uint64, k, v []byte) {
	b := &n.buckets[h%bucketsNum]
	b.lock.Lock()
	defer b.lock.Unlock()
	if dataBeginPos, ok := b.index[h]; ok {
		if _, exist := n.lookup(b, k, dataBeginPos); exist {
			n.markAsDelete(b, k, dataBeginPos)
			delete(b.index, h)
			n.maybeCompact(b)
		}
	}
	s := string(k)
	n.lock.Lock()
	_, existed := n.slow[s]
	if !existed {
		b.slowItemNum++
	}
	// 复制值，避免保存调用方的切片导致内容被后续复用篡改
	n.slow[s] = append([]byte(nil), v...)
	if !existed {
		n.appendSlowKeyLocked(s)
	}
	n.lock.Unlock()
}

// appendSlowKeyLocked 把 slow 表的 key 追加进只读日志（调用方必须持有 n.lock 写锁）。
// 日志只追加不删除，因此其中可能出现已经删除的 key —— 这符合“只多不少”的约定。
func (n *NoGcMapAny) appendSlowKeyLocked(s string) {
	cur, _ := n.slowKeys.Load().(*slowKeysView)
	if cur == nil {
		n.slowKeys.Store(&slowKeysView{keys: []string{s}})
		return
	}
	if len(cur.keys) > 2*len(n.slow)+1024 {
		// 日志膨胀：按当前 slow 表重建，避免无限增长；重建后仍要补上本次新增的 key
		keys := make([]string, 0, len(n.slow)+1)
		added := false
		for k := range n.slow {
			keys = append(keys, k)
			if k == s {
				added = true
			}
		}
		if !added {
			keys = append(keys, s)
		}
		n.slowKeys.Store(&slowKeysView{keys: keys})
		return
	}
	keys := append(cur.keys, s)
	n.slowKeys.Store(&slowKeysView{keys: keys})
}

// dropSlow 删除 slow 表中的 key（调用方必须已持有该分片的写锁）。
func (n *NoGcMapAny) dropSlow(b *bucket, k []byte) {
	s := string(k)
	n.lock.Lock()
	if _, ok := n.slow[s]; ok {
		delete(n.slow, s)
		b.slowItemNum--
	}
	n.lock.Unlock()
}

// markAsDelete 把键值对头部标记为已删除，并增加删除计数。返回 key 是否匹配。
func (n *NoGcMapAny) markAsDelete(b *bucket, k []byte, dataBeginPos uint) bool {
	chunkIndex := int(dataBeginPos) / n.chunkSize
	offset := int(dataBeginPos) % n.chunkSize
	if chunkIndex < 0 || chunkIndex >= len(b.chunks) {
		return false
	}
	chunk := b.chunks[chunkIndex]
	if offset+kvPairHeaderLen > len(chunk) {
		return false
	}
	keyLen := int(binary.BigEndian.Uint16(chunk[offset+1 : offset+3]))
	if offset+kvPairHeaderLen+keyLen > len(chunk) {
		return false
	}
	if !bytes.Equal(chunk[offset+kvPairHeaderLen:offset+kvPairHeaderLen+keyLen], k) {
		return false
	}
	chunk[offset] = 1
	b.deleteItemNum++
	b.deleteBytes += kvPairHeaderLen + keyLen + int(binary.BigEndian.Uint16(chunk[offset+3:offset+5]))
	return true
}

// lookup 在分片内定位键值对，返回的是内部存储的切片。
// 只能在持有分片锁期间使用，调用方不得持有或修改返回的切片。
func (n *NoGcMapAny) lookup(b *bucket, k []byte, dataBeginPos uint) ([]byte, bool) {
	chunkIndex := int(dataBeginPos) / n.chunkSize
	offset := int(dataBeginPos) % n.chunkSize
	if chunkIndex < 0 || chunkIndex >= len(b.chunks) {
		return nil, false
	}
	chunk := b.chunks[chunkIndex]
	if offset+kvPairHeaderLen > len(chunk) {
		return nil, false
	}
	keyLen := int(binary.BigEndian.Uint16(chunk[offset+1 : offset+3]))
	valLen := int(binary.BigEndian.Uint16(chunk[offset+3 : offset+5]))
	if keyLen != len(k) || offset+kvPairHeaderLen+keyLen+valLen > len(chunk) {
		return nil, false
	}
	if !bytes.Equal(chunk[offset+kvPairHeaderLen:offset+kvPairHeaderLen+keyLen], k) {
		return nil, false
	}
	valBegin := offset + kvPairHeaderLen + keyLen
	return chunk[valBegin : valBegin+valLen], true
}

// read 读取键值对，命中时把值追加到 dst。返回的切片不会与内部存储共享底层数组。
func (n *NoGcMapAny) read(b *bucket, k []byte, dataBeginPos uint, dst []byte) ([]byte, bool) {
	val, ok := n.lookup(b, k, dataBeginPos)
	if !ok {
		return nil, false
	}
	return append(dst, val...), true
}

// ensureWriteSpace 保证当前 chunk 能容纳 pairLen 字节；不够则新建 chunk。
// 返回是否新建了 chunk（新建后需要重新发布只读视图）。
func (n *NoGcMapAny) ensureWriteSpace(b *bucket, pairLen int) bool {
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
// 只复制 slice header，底层字节与写路径共享；写路径只向末尾追加，不覆盖已写入的字节。
func (n *NoGcMapAny) publishLocked(b *bucket) {
	chunks := make([][]byte, len(b.chunks))
	copy(chunks, b.chunks)
	lens := make([]*uint64, len(b.lens))
	copy(lens, b.lens)
	b.view.Store(&chunksView{chunks: chunks, lens: lens})
}

// write 追加一条键值对，并把 index 指向它的位置。
func (n *NoGcMapAny) write(b *bucket, h uint64, k, v []byte) {
	pairLen := len(k) + len(v) + kvPairHeaderLen
	if n.ensureWriteSpace(b, pairLen) {
		n.publishLocked(b)
	}
	chunk := b.chunks[b.curChunkIndex]
	chunk = append(chunk, 0, byte(len(k)>>8), byte(len(k)), byte(len(v)>>8), byte(len(v)))
	chunk = append(chunk, k...)
	chunk = append(chunk, v...)
	b.chunks[b.curChunkIndex] = chunk
	// 发布该 chunk 的已写入长度：先写数据、后发布长度，扫描端读到长度即能安全读到对应的数据
	atomic.StoreUint64(b.lens[b.curChunkIndex], uint64(len(chunk)))
	b.index[h] = b.dataBeginPos
	b.dataBeginPos += uint(pairLen)
}

// maybeCompact 在已删除记录占比超过阈值时，重排当前分片、回收闲置记录。
// 调用方必须持有该分片的写锁。
func (n *NoGcMapAny) maybeCompact(b *bucket) {
	if b.deleteBytes < n.compactMinBytes() {
		return
	}
	if b.dataBeginPos == 0 {
		return
	}
	// 用“被浪费的字节 / 已使用字节”判断，比按条数更贴近真实内存占用
	if b.deleteBytes*100 < int(b.dataBeginPos)*n.needDeletePercent {
		return
	}
	n.compact(b)
}

// compactMinBytes 返回触发回收所需的最小浪费字节数。
func (n *NoGcMapAny) compactMinBytes() int {
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
// 这里刻意采用“写时复制”：旧的 chunk 不会被改写，仍被无锁 key 扫描持有的旧视图可以安全读完；
// 代价是压缩时会分配与存活数据等量的新 chunk，旧 chunk 交给 GC 回收。
// 调用方必须持有该分片的写锁。
func (n *NoGcMapAny) compact(b *bucket) {
	atomic.AddUint64(&b.compactVersion, 1)       // 进入压缩（奇数）
	defer atomic.AddUint64(&b.compactVersion, 1) // 退出压缩（偶数）
	if len(b.chunks) == 0 {
		return
	}
	newChunks := make([][]byte, 0, len(b.chunks))
	newLens := make([]*uint64, 0, len(b.lens))
	newIndex := make(map[uint64]uint, len(b.index))
	pos := uint(0)
	chunkIndex := 0
	for _, chunk := range b.chunks {
		if len(chunk) == 0 {
			continue
		}
		for offset := 0; offset+kvPairHeaderLen <= len(chunk); {
			keyLen := int(binary.BigEndian.Uint16(chunk[offset+1 : offset+3]))
			valLen := int(binary.BigEndian.Uint16(chunk[offset+3 : offset+5]))
			pairLen := kvPairHeaderLen + keyLen + valLen
			if offset+pairLen > len(chunk) {
				break
			}
			if chunk[offset] == 0 {
				pair := chunk[offset : offset+pairLen]
				h := maphash.Bytes(n.seed, pair[kvPairHeaderLen:kvPairHeaderLen+keyLen])
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
				newIndex[h] = pos
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
