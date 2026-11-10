// Copyright 2020 rateLimit Author(https://github.com/yudeguang/noGCMap). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/yudeguang/noGCMap.
package noGCMap

import (
	"bytes"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// intAllocatedBytes 返回当前所有 chunk 已使用的字节数，用于观察回收效果。
func intAllocatedBytes(m *NoGcMapInt) int {
	total := 0
	for i := range m.buckets {
		for _, chunk := range m.buckets[i].chunks {
			total += len(chunk)
		}
	}
	return total
}

// intSlowItemNum 汇总各分片记录的 slow 表条目数，用于校验计数器。
func intSlowItemNum(m *NoGcMapInt) int {
	total := 0
	for i := range m.buckets {
		total += m.buckets[i].slowItemNum
	}
	return total
}

func TestIntBasic(t *testing.T) {
	m := NewInt()
	if _, ok := m.Get(1); ok {
		t.Fatal("不存在的键不应命中")
	}
	m.Set(0, []byte("zero"))
	if v, ok := m.Get(0); !ok || string(v) != "zero" {
		t.Fatalf("零值键读取失败：%q %v", v, ok)
	}
	m.Set(-1, nil) // 负键 + 空值
	if v, ok := m.Get(-1); !ok || len(v) != 0 {
		t.Fatalf("负键空值读取失败：%q %v", v, ok)
	}
	m.SetString(100, "v1")
	if v, ok := m.GetString(100); !ok || v != "v1" {
		t.Fatalf("string 读写不一致：%q %v", v, ok)
	}
	before := intAllocatedBytes(m)
	m.SetString(100, "v1")
	if after := intAllocatedBytes(m); after != before {
		t.Fatalf("写入相同值不应追加记录：before=%d after=%d", before, after)
	}
	m.SetString(100, "v2")
	if v, ok := m.GetString(100); !ok || v != "v2" {
		t.Fatalf("更新后读取不一致：%q %v", v, ok)
	}
	if m.Len() != 3 {
		t.Fatalf("Len()=%d, want 3", m.Len())
	}
	m.Delete(100)
	if _, ok := m.GetString(100); ok {
		t.Fatal("删除后不应命中")
	}
	if m.Len() != 2 {
		t.Fatalf("删除后 Len()=%d, want 2", m.Len())
	}
	m.Delete(999999)
}

// 回归 P1：长值迁移与删除后不得“复活”
func TestIntLongKVAndMigration(t *testing.T) {
	m := NewInt()
	long := bytes.Repeat([]byte("A"), 70000)
	m.Set(7, []byte("v-short"))
	m.Set(7, long)
	if v, ok := m.Get(7); !ok || !bytes.Equal(v, long) {
		t.Fatalf("短值改长值后应读到新值：len=%d ok=%v", len(v), ok)
	}
	if m.Len() != 1 {
		t.Fatalf("同一个键被重复计数：Len()=%d", m.Len())
	}
	m.Set(7, []byte("v-short"))
	if v, ok := m.Get(7); !ok || string(v) != "v-short" {
		t.Fatalf("长值改短值后应读到新值：%q ok=%v", v, ok)
	}
	if m.Len() != 1 {
		t.Fatalf("长值改短值后重复计数：Len()=%d", m.Len())
	}
	m.Set(7, long)
	m.Delete(7)
	if v, ok := m.Get(7); ok {
		t.Fatalf("删除后数据复活：len=%d", len(v))
	}
	if m.Len() != 0 || intSlowItemNum(m) != 0 {
		t.Fatalf("删除后残留：Len()=%d slowItemNum=%d", m.Len(), intSlowItemNum(m))
	}
	m.SetString(8, string(long))
	m.Delete(8)
	if _, ok := m.GetString(8); ok {
		t.Fatal("慢表数据删除后复活")
	}
}

// 回归 P1：切片别名
func TestIntSliceAliasing(t *testing.T) {
	m := NewInt()
	val := bytes.Repeat([]byte("A"), 70000)
	m.Set(1, val)
	val[0] = 'Z'
	if got, _ := m.Get(1); got[0] != 'A' {
		t.Fatal("Set 未复制值")
	}
	got, _ := m.Get(1)
	got[0] = 'X'
	if got2, _ := m.Get(1); got2[0] != 'A' {
		t.Fatal("Get 返回了内部切片")
	}
	small := []byte("abc")
	m.Set(2, small)
	small[0] = 'z'
	if v, _ := m.Get(2); string(v) != "abc" {
		t.Fatalf("分片路径未复制键值：%q", v)
	}
}

// 回归 P1：不同 chunk 配置的实例互不影响
func TestIntConfigIsolation(t *testing.T) {
	a := NewInt()
	b, err := NewIntWithError(Config{MultiplesOf64KForchunkSize: 4})
	if err != nil {
		t.Fatalf("创建实例失败：%v", err)
	}
	if a.ChunkSize() != 64*1024 || b.ChunkSize() != 256*1024 {
		t.Fatalf("实例配置未隔离：%d / %d", a.ChunkSize(), b.ChunkSize())
	}
	val := bytes.Repeat([]byte("A"), 600)
	for i := 0; i < 70000; i++ {
		a.Set(i, val)
		b.Set(i, val)
	}
	for i := 0; i < 70000; i++ {
		v, ok := a.Get(i)
		if !ok || len(v) != 600 {
			t.Fatalf("实例 a 第 %d 条读取失败：ok=%v len=%d", i, ok, len(v))
		}
		v, ok = b.Get(i)
		if !ok || len(v) != 600 {
			t.Fatalf("实例 b 第 %d 条读取失败：ok=%v len=%d", i, ok, len(v))
		}
	}
}

// 回归 P2：仅覆盖更新也要触发回收
func TestIntUpdateTriggersCompaction(t *testing.T) {
	m := NewInt()
	const rounds = 20000
	for i := 0; i < rounds; i++ {
		m.Set(1, []byte(strconv.Itoa(i)))
	}
	if m.Len() != 1 {
		t.Fatalf("Len()=%d, want 1", m.Len())
	}
	if v, ok := m.Get(1); !ok || string(v) != strconv.Itoa(rounds-1) {
		t.Fatalf("最后一次写入不可见：%q %v", v, ok)
	}
	if used := intAllocatedBytes(m); used > 4*64*1024 {
		t.Fatalf("覆盖更新未回收：占用 %d 字节", used)
	}
}

// 回归 P1/P2：压缩后数据完整且可继续写入
func TestIntCompactionKeepsData(t *testing.T) {
	m := NewInt()
	const total = 5000
	for i := 0; i < total; i++ {
		m.SetString(i, "@"+strconv.Itoa(i))
	}
	// 捕获压缩前的视图，压缩后旧视图仍应完整可读（长度与数据同属一份快照）
	captured := make([]*chunksViewInt, bucketsNum)
	for i := range m.buckets {
		captured[i], _ = m.buckets[i].view.Load().(*chunksViewInt)
	}
	for i := 0; i < 4000; i++ {
		m.Delete(i)
	}
	fromOld := make(map[int]struct{})
	for i := range captured {
		if captured[i] == nil {
			continue
		}
		for _, k := range parseChunkKeysInt(captured[i], m.chunkSize, nil) {
			fromOld[k] = struct{}{}
		}
	}
	for i := 0; i < total; i++ {
		if _, ok := fromOld[i]; !ok {
			t.Fatalf("压缩后旧视图漏读 key：%d", i)
		}
	}
	for i := 0; i < total; i++ {
		v, ok := m.GetString(i)
		if i < 4000 {
			if ok {
				t.Fatalf("已删除的键仍可见：%d", i)
			}
			continue
		}
		if !ok || v != "@"+strconv.Itoa(i) {
			t.Fatalf("压缩后数据不一致：%d %q %v", i, v, ok)
		}
	}
	if m.Len() != 1000 {
		t.Fatalf("Len()=%d, want 1000", m.Len())
	}
	for i := 0; i < 3000; i++ {
		m.SetString(total+i, "v"+strconv.Itoa(i))
	}
	for i := 0; i < 3000; i++ {
		if v, ok := m.GetString(total + i); !ok || v != "v"+strconv.Itoa(i) {
			t.Fatalf("压缩后继续写入不一致：%d %q %v", i, v, ok)
		}
	}
}

// 回归 P2：GetTo 零分配
func TestIntGetToZeroAlloc(t *testing.T) {
	m := NewInt()
	value := bytes.Repeat([]byte("v"), 256)
	m.Set(9, value)
	dst := make([]byte, 0, 512)
	allocs := testing.AllocsPerRun(100, func() {
		v, ok := m.GetTo(dst[:0], 9)
		if !ok || len(v) != 256 {
			t.Fatalf("GetTo 读取失败：len=%d ok=%v", len(v), ok)
		}
	})
	if allocs != 0 {
		t.Fatalf("GetTo 仍有分配：%v allocs/op", allocs)
	}
}

// 并发读写删
func TestIntConcurrent(t *testing.T) {
	m := NewInt()
	const goroutines = 8
	const perGoroutine = 2000
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done() // goroutine 内只用 t.Errorf
			base := g * perGoroutine
			for i := 0; i < perGoroutine; i++ {
				k := base + i
				m.SetString(k, "v"+strconv.Itoa(i))
				if v, ok := m.GetString(k); !ok || v != "v"+strconv.Itoa(i) {
					t.Errorf("并发读写不一致：%d %q %v", k, v, ok)
					return
				}
				if i%3 == 0 {
					m.Delete(k)
					if _, ok := m.GetString(k); ok {
						t.Errorf("并发删除后仍可见：%d", k)
						return
					}
				}
			}
		}(g)
	}
	var stop int32
	var counter sync.WaitGroup
	counter.Add(1)
	go func() {
		defer counter.Done()
		for atomic.LoadInt32(&stop) == 0 {
			_ = m.Len()
		}
	}()
	wg.Wait()
	atomic.StoreInt32(&stop, 1)
	counter.Wait()
}

// 随机差分测试：含负键、长值、更新与删除
func TestIntRandomDifferential(t *testing.T) {
	m := NewInt()
	ref := make(map[int][]byte)
	r := rand.New(rand.NewSource(20261111))
	longValue := bytes.Repeat([]byte("L"), 70000)
	for op := 0; op < 200000; op++ {
		k := r.Intn(6000) - 3000 // 覆盖负键
		switch r.Intn(10) {
		case 0, 1, 2, 3, 4:
			var v []byte
			if r.Intn(200) == 0 {
				v = longValue
			} else {
				v = bytes.Repeat([]byte("v"), r.Intn(300))
			}
			m.Set(k, v)
			ref[k] = append([]byte(nil), v...)
		case 5, 6, 7:
			m.Delete(k)
			delete(ref, k)
		}
		got, ok := m.Get(k)
		want, wantOK := ref[k]
		if ok != wantOK || !bytes.Equal(got, want) {
			t.Fatalf("op=%d key=%d 不一致：got=(len %d, %v) want=(len %d, %v)", op, k, len(got), ok, len(want), wantOK)
		}
	}
	if m.Len() != len(ref) {
		t.Fatalf("Len()=%d, want %d", m.Len(), len(ref))
	}
	if got, want := intSlowItemNum(m), len(m.slow); got != want {
		t.Fatalf("slow 计数不一致：slowItemNum=%d len(slow)=%d", got, want)
	}
}

// 较大规模压力测试：默认跳过 -short
func TestIntStress(t *testing.T) {
	if testing.Short() {
		t.Skip("short 模式跳过压力测试")
	}
	m := NewInt()
	const total = 300000
	const goroutines = 8
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < total; i++ {
				m.SetString(i, "@"+strconv.Itoa(i))
			}
		}(g)
	}
	wg.Wait()
	if m.Len() != total {
		t.Fatalf("Len()=%d, want %d", m.Len(), total)
	}
	for i := 0; i < total; i += 97 {
		if v, ok := m.GetString(i); !ok || v != "@"+strconv.Itoa(i) {
			t.Fatalf("压力测试后数据不一致：%d %q %v", i, v, ok)
		}
	}
}
