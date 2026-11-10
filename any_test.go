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

// anyAllocatedBytes 返回当前所有 chunk 已使用的字节数，用于观察回收效果。
func anyAllocatedBytes(m *NoGcMapAny) int {
	total := 0
	for i := range m.buckets {
		for _, chunk := range m.buckets[i].chunks {
			total += len(chunk)
		}
	}
	return total
}

// anySlowItemNum 汇总各分片记录的 slow 表条目数，用于校验计数器。
func anySlowItemNum(m *NoGcMapAny) int {
	total := 0
	for i := range m.buckets {
		total += m.buckets[i].slowItemNum
	}
	return total
}

func TestAnyBasic(t *testing.T) {
	m := New()
	if _, ok := m.Get([]byte("missing")); ok {
		t.Fatal("不存在的键不应命中")
	}
	m.Set([]byte(""), []byte("empty-key"))
	if v, ok := m.Get([]byte("")); !ok || string(v) != "empty-key" {
		t.Fatalf("空键读取失败：%q %v", v, ok)
	}
	m.Set([]byte("empty-value"), nil)
	if v, ok := m.Get([]byte("empty-value")); !ok || len(v) != 0 {
		t.Fatalf("空值读取失败：%q %v", v, ok)
	}
	m.SetString("k", "v1")
	if v, ok := m.GetString("k"); !ok || v != "v1" {
		t.Fatalf("string 读写不一致：%q %v", v, ok)
	}
	// 相同值的重复写入不应改变长度与内容
	before := anyAllocatedBytes(m)
	m.SetString("k", "v1")
	if after := anyAllocatedBytes(m); after != before {
		t.Fatalf("写入相同值不应追加记录：before=%d after=%d", before, after)
	}
	// 更新
	m.SetString("k", "v2")
	if v, ok := m.GetString("k"); !ok || v != "v2" {
		t.Fatalf("更新后读取不一致：%q %v", v, ok)
	}
	if m.Len() != 3 {
		t.Fatalf("Len()=%d, want 3", m.Len())
	}
	// 删除
	m.DeleteString("k")
	if _, ok := m.GetString("k"); ok {
		t.Fatal("删除后不应命中")
	}
	if m.Len() != 2 {
		t.Fatalf("删除后 Len()=%d, want 2", m.Len())
	}
	m.DeleteString("never-exist")
}

// 回归 P1：短值→长值、长值→短值之间的迁移，以及删除后不得“复活”
func TestAnyLongKVAndMigration(t *testing.T) {
	m := New()
	key := []byte("k1")
	short := []byte("v-short")
	long := bytes.Repeat([]byte("A"), 70000) // 超过默认 chunkSize，只能走 slow 表

	m.Set(key, short)
	m.Set(key, long)
	if v, ok := m.Get(key); !ok || !bytes.Equal(v, long) {
		t.Fatalf("短值改长值后应读到新值：len=%d ok=%v", len(v), ok)
	}
	if m.Len() != 1 {
		t.Fatalf("同一个键被重复计数：Len()=%d", m.Len())
	}

	m.Set(key, short) // 长值改回短值，必须清理 slow 表
	if v, ok := m.Get(key); !ok || !bytes.Equal(v, short) {
		t.Fatalf("长值改短值后应读到新值：%q ok=%v", v, ok)
	}
	if m.Len() != 1 {
		t.Fatalf("长值改短值后重复计数：Len()=%d", m.Len())
	}

	m.Set(key, long)
	m.Delete(key)
	if v, ok := m.Get(key); ok {
		t.Fatalf("删除后数据复活：len=%d", len(v))
	}
	if m.Len() != 0 {
		t.Fatalf("删除后 Len()=%d, want 0", m.Len())
	}
	if got := anySlowItemNum(m); got != 0 {
		t.Fatalf("slow 表计数残留：%d", got)
	}

	// 只存在于慢表的键，删除后同样不能复活
	m.SetString("only-long", string(long))
	m.DeleteString("only-long")
	if _, ok := m.GetString("only-long"); ok {
		t.Fatal("慢表数据删除后复活")
	}
}

// 回归 P1：返回/传入切片的别名问题
func TestAnySliceAliasing(t *testing.T) {
	m := New()
	long := bytes.Repeat([]byte("A"), 70000)
	val := append([]byte(nil), long...)
	m.Set([]byte("big"), val)
	val[0] = 'Z'
	if got, _ := m.Get([]byte("big")); got[0] != 'A' {
		t.Fatal("Set 未复制值：调用方修改缓冲区后缓存内容被篡改")
	}
	got, _ := m.Get([]byte("big"))
	got[0] = 'X'
	if got2, _ := m.Get([]byte("big")); got2[0] != 'A' {
		t.Fatal("Get 返回了内部切片：修改返回值污染了缓存")
	}

	// 分片路径同样必须隔离
	small := []byte("abc")
	m.Set([]byte("s"), small)
	small[0] = 'z'
	if v, _ := m.Get([]byte("s")); string(v) != "abc" {
		t.Fatalf("分片路径未复制键值：%q", v)
	}
	v, _ := m.Get([]byte("s"))
	v[0] = 'q'
	if v2, _ := m.Get([]byte("s")); string(v2) != "abc" {
		t.Fatalf("分片路径返回内部切片：%q", v2)
	}
}

// 回归 P1：实例配置必须相互隔离，不能互相影响
func TestAnyConfigIsolation(t *testing.T) {
	small := New() // 64K
	big, err := NewWithError(Config{MultiplesOf64KForchunkSize: 2})
	if err != nil {
		t.Fatalf("创建 128K 实例失败：%v", err)
	}
	if small.ChunkSize() != 64*1024 || big.ChunkSize() != 128*1024 {
		t.Fatalf("实例配置未隔离：%d / %d", small.ChunkSize(), big.ChunkSize())
	}
	val := bytes.Repeat([]byte("A"), 600)
	for i := 0; i < 70000; i++ {
		small.Set([]byte("k"+strconv.Itoa(i)), val)
	}
	// 创建并密集写入第二个实例，覆盖到 128K 块边界
	for i := 0; i < 70000; i++ {
		big.Set([]byte("k"+strconv.Itoa(i)), val)
	}
	for i := 0; i < 70000; i++ {
		v, ok := small.Get([]byte("k" + strconv.Itoa(i)))
		if !ok || len(v) != 600 {
			t.Fatalf("小实例第 %d 条读取失败：ok=%v len=%d", i, ok, len(v))
		}
	}
	// 并发创建实例不应破坏已有实例的数据
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = New(Config{MultiplesOf64KForchunkSize: 1 + j%4})
			}
		}(i)
	}
	wg.Wait()
	if v, ok := small.Get([]byte("k1")); !ok || len(v) != 600 {
		t.Fatal("并发创建实例后已有实例数据异常")
	}
}

// 回归 P3：配置边界必须可被感知
func TestAnyConfigValidation(t *testing.T) {
	cases := []Config{
		{NeedDeketeKVPairPersent: -1},
		{NeedDeketeKVPairPersent: 101},
		{MultiplesOf64KForchunkSize: -1},
		{MultiplesOf64KForchunkSize: 1001},
	}
	for _, c := range cases {
		if _, err := NewWithError(c); err == nil {
			t.Fatalf("非法配置未被拒绝：%+v", c)
		}
		if _, err := NewIntWithError(c); err == nil {
			t.Fatalf("int 版非法配置未被拒绝：%+v", c)
		}
	}
	// New 保持历史行为：非法配置回退默认值，不 panic
	m := New(Config{MultiplesOf64KForchunkSize: 1001})
	if m.ChunkSize() != 64*1024 {
		t.Fatalf("非法配置未回退默认值：%d", m.ChunkSize())
	}
	m.SetString("k", "v")
	if v, ok := m.GetString("k"); !ok || v != "v" {
		t.Fatal("非法配置回退后实例不可用")
	}
	// 边界内的极值可用
	if _, err := NewWithError(Config{MultiplesOf64KForchunkSize: 1, NeedDeketeKVPairPersent: 100}); err != nil {
		t.Fatalf("合法边界值被拒绝：%v", err)
	}
}

// 回归 P2：只做覆盖更新（不调用 Delete）也必须触发回收
func TestAnyUpdateTriggersCompaction(t *testing.T) {
	m := New()
	key := []byte("k")
	const rounds = 20000
	for i := 0; i < rounds; i++ {
		m.Set(key, []byte(strconv.Itoa(i)))
	}
	if m.Len() != 1 {
		t.Fatalf("Len()=%d, want 1", m.Len())
	}
	if v, ok := m.Get(key); !ok || string(v) != strconv.Itoa(rounds-1) {
		t.Fatalf("最后一次写入不可见：%q %v", v, ok)
	}
	// 单键 2 万次更新，压缩后占用应远小于“每条记录都保留”的量级
	if used := anyAllocatedBytes(m); used > 4*64*1024 {
		t.Fatalf("覆盖更新未回收：占用 %d 字节", used)
	}
}

// 回归 P1/P2：压缩后数据必须完整，且可以继续写入
func TestAnyCompactionKeepsData(t *testing.T) {
	m := New()
	const total = 5000
	for i := 0; i < total; i++ {
		m.SetString(strconv.Itoa(i), "@"+strconv.Itoa(i))
	}
	for i := 0; i < 4000; i++ { // 足以触发多轮压缩
		m.DeleteString(strconv.Itoa(i))
	}
	for i := 0; i < total; i++ {
		v, ok := m.GetString(strconv.Itoa(i))
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
		m.SetString("new"+strconv.Itoa(i), "v"+strconv.Itoa(i))
	}
	for i := 0; i < 3000; i++ {
		if v, ok := m.GetString("new" + strconv.Itoa(i)); !ok || v != "v"+strconv.Itoa(i) {
			t.Fatalf("压缩后继续写入不一致：%d %q %v", i, v, ok)
		}
	}
}

// 回归 P2：GetTo 在复用缓冲区时应做到零分配
func TestAnyGetToZeroAlloc(t *testing.T) {
	m := New()
	key := []byte("k")
	value := bytes.Repeat([]byte("v"), 256)
	m.Set(key, value)
	dst := make([]byte, 0, 512)
	allocs := testing.AllocsPerRun(100, func() {
		v, ok := m.GetTo(dst[:0], key)
		if !ok || len(v) != 256 {
			t.Fatalf("GetTo 读取失败：len=%d ok=%v", len(v), ok)
		}
	})
	if allocs != 0 {
		t.Fatalf("GetTo 仍有分配：%v allocs/op", allocs)
	}
	// 使用 dst 的读取结果可以与内部存储解耦
	v, _ := m.GetTo(dst[:0], key)
	v[0] = 'X'
	if again, _ := m.Get(key); again[0] != 'v' {
		t.Fatal("GetTo 结果与内部存储共享了底层数组")
	}
}

// 并发读写删：在 -race 下同样应当无竞态
func TestAnyConcurrent(t *testing.T) {
	m := New()
	const goroutines = 8
	const perGoroutine = 2000
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done() // 注意：goroutine 内只用 t.Errorf，不使用 t.Fatalf
			prefix := "g" + strconv.Itoa(g) + "-"
			for i := 0; i < perGoroutine; i++ {
				k := prefix + strconv.Itoa(i)
				m.SetString(k, "v"+strconv.Itoa(i))
				if v, ok := m.GetString(k); !ok || v != "v"+strconv.Itoa(i) {
					t.Errorf("并发读写不一致：%s %q %v", k, v, ok)
					return
				}
				if i%3 == 0 {
					m.DeleteString(k)
					if _, ok := m.GetString(k); ok {
						t.Errorf("并发删除后仍可见：%s", k)
						return
					}
				}
			}
		}(g)
	}
	// 并发写的同时反复统计长度，验证不会卡死或 panic
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

// 随机差分测试：与标准 map 逐条比对，覆盖更新、删除、长短值迁移与多轮压缩
func TestAnyRandomDifferential(t *testing.T) {
	m := New()
	ref := make(map[string][]byte)
	r := rand.New(rand.NewSource(20261110))
	longValue := bytes.Repeat([]byte("L"), 70000)
	for op := 0; op < 200000; op++ {
		key := "k" + strconv.Itoa(r.Intn(3000))
		switch r.Intn(10) {
		case 0, 1, 2, 3, 4:
			var v []byte
			if r.Intn(200) == 0 {
				v = longValue
			} else {
				v = bytes.Repeat([]byte("v"), r.Intn(300))
			}
			m.Set([]byte(key), v)
			ref[key] = append([]byte(nil), v...)
		case 5, 6, 7:
			m.Delete([]byte(key))
			delete(ref, key)
		}
		got, ok := m.Get([]byte(key))
		want, wantOK := ref[key]
		if ok != wantOK || !bytes.Equal(got, want) {
			t.Fatalf("op=%d key=%s 不一致：got=(len %d, %v) want=(len %d, %v)", op, key, len(got), ok, len(want), wantOK)
		}
	}
	if m.Len() != len(ref) {
		t.Fatalf("Len()=%d, want %d", m.Len(), len(ref))
	}
	if got, want := anySlowItemNum(m), len(m.slow); got != want {
		t.Fatalf("slow 计数不一致：slowItemNum=%d len(slow)=%d", got, want)
	}
}

// 较大规模的压力测试：默认跳过 -short
func TestAnyStress(t *testing.T) {
	if testing.Short() {
		t.Skip("short 模式跳过压力测试")
	}
	m := New()
	const total = 300000
	const goroutines = 8
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < total; i++ {
				m.SetString(strconv.Itoa(i), "@"+strconv.Itoa(i))
			}
		}(g)
	}
	wg.Wait()
	if m.Len() != total {
		t.Fatalf("Len()=%d, want %d", m.Len(), total)
	}
	for i := 0; i < total; i += 97 {
		if v, ok := m.GetString(strconv.Itoa(i)); !ok || v != "@"+strconv.Itoa(i) {
			t.Fatalf("压力测试后数据不一致：%d %q %v", i, v, ok)
		}
	}
}
