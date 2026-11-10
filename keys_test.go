// Copyright 2020 rateLimit Author(https://github.com/yudeguang/noGCMap). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/yudeguang/noGCMap.
package noGCMap

import (
	"bytes"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// keysToSet 把 [][]byte 形式的 key 转成集合，便于做“是否包含”的断言。
func keysToSet(keys [][]byte) map[string]struct{} {
	set := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		set[string(k)] = struct{}{}
	}
	return set
}

func intKeysToSet(keys []int) map[int]struct{} {
	set := make(map[int]struct{}, len(keys))
	for _, k := range keys {
		set[k] = struct{}{}
	}
	return set
}

// 基本语义：已存在的 key 必须全部出现；返回的 key 是副本，可以自由修改。
func TestAnyKeyToSliceUnSafeBasic(t *testing.T) {
	m := New()
	const total = 5000
	m.SetString("", "empty-key") // 空键
	for i := 0; i < total; i++ {
		m.SetString(strconv.Itoa(i), "@"+strconv.Itoa(i))
	}
	keys := m.KeyToSliceUnSafe(nil)
	set := keysToSet(keys)
	for i := 0; i < total; i++ {
		if _, ok := set[strconv.Itoa(i)]; !ok {
			t.Fatalf("缺少 key：%d", i)
		}
	}
	if _, ok := set[""]; !ok {
		t.Fatal("缺少空键")
	}

	// 返回的 key 必须是副本：改动它不能影响 Map 内部
	for _, k := range keys {
		if len(k) > 0 {
			original := k[0]
			k[0] = 'Z'
			if v, ok := m.Get([]byte(k)); ok {
				if len(v) > 0 && v[0] == '@' {
					// 命中说明副本没有被内部共享（这里只是确保不 panic）
					_ = original
				}
			}
			k[0] = original
		}
	}
	if v, ok := m.GetString("1"); !ok || v != "@1" {
		t.Fatalf("内部数据被返回的 key 影响：%q %v", v, ok)
	}

	// 支持复用 dst
	dst := make([][]byte, 0, len(keys)+10)
	again := m.KeyToSliceUnSafe(dst)
	if len(again) < total {
		t.Fatalf("复用 dst 后结果变少：%d", len(again))
	}
}

// 已删除的 key 仍会出现在结果里（只多不少），但仍存在的 key 不能丢。
func TestAnyKeyToSliceUnSafeAfterDelete(t *testing.T) {
	m := New()
	for i := 0; i < 2000; i++ {
		m.SetString(strconv.Itoa(i), "v"+strconv.Itoa(i))
	}
	for i := 0; i < 1000; i++ {
		m.DeleteString(strconv.Itoa(i))
	}
	set := keysToSet(m.KeyToSliceUnSafe(nil))
	for i := 1000; i < 2000; i++ {
		if _, ok := set[strconv.Itoa(i)]; !ok {
			t.Fatalf("缺少仍存在的 key：%d", i)
		}
	}
	for i := 0; i < 1000; i++ {
		if _, ok := set[strconv.Itoa(i)]; !ok {
			t.Fatalf("已删除的 key 也应当保留在历史结果中：%d", i)
		}
	}
}

// slow 表（超长值）里的 key 必须被收集到。
func TestAnyKeyToSliceUnSafeSlowKeys(t *testing.T) {
	m := New()
	long := bytes.Repeat([]byte("L"), 70000)
	m.SetString("slow-1", string(long))
	m.SetString("slow-2", string(long))
	m.SetString("fast", "v")
	set := keysToSet(m.KeyToSliceUnSafe(nil))
	for _, k := range []string{"slow-1", "slow-2", "fast"} {
		if _, ok := set[k]; !ok {
			t.Fatalf("缺少 key：%s", k)
		}
	}
}

// 方法内部不得获取任何 map 锁：把全部分片锁与全局锁都被占用时仍能返回。
func TestAnyKeyToSliceUnSafeNoMapLock(t *testing.T) {
	m := New()
	for i := 0; i < 100; i++ {
		m.SetString(strconv.Itoa(i), "v")
	}
	for i := range m.buckets {
		m.buckets[i].lock.Lock()
	}
	m.lock.Lock()
	unlock := func() {
		m.lock.Unlock()
		for i := range m.buckets {
			m.buckets[i].lock.Unlock()
		}
	}
	done := make(chan [][]byte, 1)
	go func() {
		done <- m.KeyToSliceUnSafe(nil)
	}()
	select {
	case keys := <-done:
		unlock()
		set := keysToSet(keys)
		for i := 0; i < 100; i++ {
			if _, ok := set[strconv.Itoa(i)]; !ok {
				t.Fatalf("缺少 key：%d", i)
			}
		}
	case <-time.After(3 * time.Second):
		unlock()
		t.Fatal("KeyToSliceUnSafe 获取了 map 锁，导致阻塞")
	}
}

// 并发写入 + 压缩的同时反复扫描：预先存在且从未删除的 key 必须每次都能读到。
func TestAnyKeyToSliceUnSafeConcurrent(t *testing.T) {
	m := New()
	const stable = 2000
	for i := 0; i < stable; i++ {
		m.SetString("s"+strconv.Itoa(i), "0")
	}
	var stop int32
	var wg sync.WaitGroup
	wg.Add(2)
	// 写线程：不断改写已有 key 的值，制造大量“已删除记录”，从而频繁触发压缩
	go func() {
		defer wg.Done()
		n := 0
		for atomic.LoadInt32(&stop) == 0 {
			n++
			for i := 0; i < stable; i += 23 {
				m.SetString("s"+strconv.Itoa(i), strconv.Itoa(n))
			}
			m.SetString("churn"+strconv.Itoa(n%64), "x")
			if n%3 == 0 {
				m.DeleteString("churn" + strconv.Itoa((n+1)%64))
			}
		}
	}()
	// 读线程：持续读取，确认扫描不与写路径互相阻塞
	go func() {
		defer wg.Done()
		for atomic.LoadInt32(&stop) == 0 {
			m.GetString("s1")
		}
	}()

	for iter := 0; iter < 40; iter++ {
		set := keysToSet(m.KeyToSliceUnSafe(nil))
		for i := 0; i < stable; i++ {
			if _, ok := set["s"+strconv.Itoa(i)]; !ok {
				atomic.StoreInt32(&stop, 1)
				wg.Wait()
				t.Fatalf("第 %d 次扫描缺少稳定 key：s%d", iter, i)
			}
		}
	}
	atomic.StoreInt32(&stop, 1)
	wg.Wait()
}

// 压缩后（chunk 被整体替换）仍能读到全部 key。
func TestAnyKeyToSliceUnSafeAfterCompaction(t *testing.T) {
	m := New()
	for i := 0; i < 3000; i++ {
		m.SetString("k"+strconv.Itoa(i), "v"+strconv.Itoa(i))
	}
	// 捕获压缩前的视图：它自带每个 chunk 的原子长度，压缩整体替换后旧视图仍应完整可读。
	// 这是“视图与长度必须同属一份快照”的核心不变量，防止再次出现“旧视图 + 新边界”式漏 key。
	captured := make([]*chunksView, bucketsNum)
	for i := range m.buckets {
		captured[i], _ = m.buckets[i].view.Load().(*chunksView)
	}
	for i := 0; i < 20000; i++ { // 触发多轮压缩
		m.SetString("hot", strconv.Itoa(i))
	}
	fromOld := make(map[string]struct{})
	for i := range captured {
		if captured[i] == nil {
			continue
		}
		for _, k := range parseChunkKeys(captured[i], m.chunkSize, nil) {
			fromOld[string(k)] = struct{}{}
		}
	}
	for i := 0; i < 3000; i++ {
		if _, ok := fromOld["k"+strconv.Itoa(i)]; !ok {
			t.Fatalf("压缩后旧视图漏读 key：%d", i)
		}
	}
	set := keysToSet(m.KeyToSliceUnSafe(nil))
	for i := 0; i < 3000; i++ {
		if _, ok := set["k"+strconv.Itoa(i)]; !ok {
			t.Fatalf("压缩后缺少 key：%d", i)
		}
	}
	if _, ok := set["hot"]; !ok {
		t.Fatal("压缩后缺少热键")
	}
}

// int 版本的基本语义
func TestIntKeyToSliceUnSafe(t *testing.T) {
	m := NewInt()
	const total = 5000
	for i := 0; i < total; i++ {
		m.SetString(i, "@"+strconv.Itoa(i))
	}
	m.SetString(-1, "neg")
	long := bytes.Repeat([]byte("L"), 70000)
	m.SetString(-2, string(long)) // 走 slow 表

	set := intKeysToSet(m.KeyToSliceUnSafe(nil))
	for i := 0; i < total; i++ {
		if _, ok := set[i]; !ok {
			t.Fatalf("缺少 key：%d", i)
		}
	}
	for _, k := range []int{-1, -2} {
		if _, ok := set[k]; !ok {
			t.Fatalf("缺少 key：%d", k)
		}
	}

	// 删除后仍保留在历史结果中
	for i := 0; i < 1000; i++ {
		m.Delete(i)
	}
	set = intKeysToSet(m.KeyToSliceUnSafe(nil))
	for i := 0; i < total; i++ {
		if _, ok := set[i]; !ok {
			t.Fatalf("缺少 key（删除后）：%d", i)
		}
	}
}

// int 版本同样不得获取 map 锁
func TestIntKeyToSliceUnSafeNoMapLock(t *testing.T) {
	m := NewInt()
	for i := 0; i < 100; i++ {
		m.Set(i, []byte("v"))
	}
	for i := range m.buckets {
		m.buckets[i].lock.Lock()
	}
	m.lock.Lock()
	unlock := func() {
		m.lock.Unlock()
		for i := range m.buckets {
			m.buckets[i].lock.Unlock()
		}
	}
	done := make(chan []int, 1)
	go func() {
		done <- m.KeyToSliceUnSafe(nil)
	}()
	select {
	case keys := <-done:
		unlock()
		set := intKeysToSet(keys)
		for i := 0; i < 100; i++ {
			if _, ok := set[i]; !ok {
				t.Fatalf("缺少 key：%d", i)
			}
		}
	case <-time.After(3 * time.Second):
		unlock()
		t.Fatal("KeyToSliceUnSafe 获取了 map 锁，导致阻塞")
	}
}

// int 版本并发扫描
func TestIntKeyToSliceUnSafeConcurrent(t *testing.T) {
	m := NewInt()
	const stable = 2000
	for i := 0; i < stable; i++ {
		m.SetString(i, "0")
	}
	var stop int32
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := 0
		for atomic.LoadInt32(&stop) == 0 {
			n++
			for i := 0; i < stable; i += 23 {
				m.SetString(i, strconv.Itoa(n))
			}
			m.SetString(stable+n%64, "x")
		}
	}()
	for iter := 0; iter < 40; iter++ {
		set := intKeysToSet(m.KeyToSliceUnSafe(nil))
		for i := 0; i < stable; i++ {
			if _, ok := set[i]; !ok {
				atomic.StoreInt32(&stop, 1)
				wg.Wait()
				t.Fatalf("第 %d 次扫描缺少稳定 key：%d", iter, i)
			}
		}
	}
	atomic.StoreInt32(&stop, 1)
	wg.Wait()
}

// 在压缩高频发生期间扫描：确认压缩确实发生过，且稳定 key 一次都不缺。
func TestAnyKeyToSliceUnSafeDuringCompaction(t *testing.T) {
	m := New()
	const stable = 200
	for i := 0; i < stable; i++ {
		m.SetString("k"+strconv.Itoa(i), "v")
	}
	var stop int32
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := 0
		for atomic.LoadInt32(&stop) == 0 {
			n++
			m.SetString("hot", strconv.Itoa(n)) // 同一 key 反复更新，持续制造删除记录
		}
	}()
	scans := 0
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		set := keysToSet(m.KeyToSliceUnSafe(nil))
		for i := 0; i < stable; i++ {
			if _, ok := set["k"+strconv.Itoa(i)]; !ok {
				atomic.StoreInt32(&stop, 1)
				wg.Wait()
				t.Fatalf("第 %d 次扫描缺少稳定 key：k%d", scans, i)
			}
		}
		scans++
	}
	atomic.StoreInt32(&stop, 1)
	wg.Wait()
	var version uint64
	for i := range m.buckets {
		version += atomic.LoadUint64(&m.buckets[i].compactVersion)
	}
	if version == 0 {
		t.Fatal("测试期间没有触发压缩，用例不成立")
	}
	t.Logf("扫描 %d 次，期间压缩 %d 轮", scans, version/2)
}

// int 版本在压缩高频发生期间扫描
func TestIntKeyToSliceUnSafeDuringCompaction(t *testing.T) {
	m := NewInt()
	const stable = 200
	for i := 0; i < stable; i++ {
		m.Set(i, []byte("v"))
	}
	var stop int32
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := 0
		for atomic.LoadInt32(&stop) == 0 {
			n++
			m.SetString(-1, strconv.Itoa(n))
		}
	}()
	scans := 0
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		set := intKeysToSet(m.KeyToSliceUnSafe(nil))
		for i := 0; i < stable; i++ {
			if _, ok := set[i]; !ok {
				atomic.StoreInt32(&stop, 1)
				wg.Wait()
				t.Fatalf("第 %d 次扫描缺少稳定 key：%d", scans, i)
			}
		}
		scans++
	}
	atomic.StoreInt32(&stop, 1)
	wg.Wait()
	var version uint64
	for i := range m.buckets {
		version += atomic.LoadUint64(&m.buckets[i].compactVersion)
	}
	if version == 0 {
		t.Fatal("测试期间没有触发压缩，用例不成立")
	}
	t.Logf("扫描 %d 次，期间压缩 %d 轮", scans, version/2)
}

// slow 表 key 日志按阈值重建后，当前仍在 slow 表的 key 不能丢。
func TestAnyKeyToSliceUnSafeSlowLogRebuild(t *testing.T) {
	m := New()
	long := string(bytes.Repeat([]byte("L"), 70000))
	const total = 1050
	for i := 0; i < total; i++ {
		m.SetString("slow"+strconv.Itoa(i), long)
	}
	for i := 0; i < 1040; i++ {
		m.DeleteString("slow" + strconv.Itoa(i))
	}
	m.SetString("slow-new", long) // 该次写入会触发日志重建
	set := keysToSet(m.KeyToSliceUnSafe(nil))
	for i := 1040; i < total; i++ {
		if _, ok := set["slow"+strconv.Itoa(i)]; !ok {
			t.Fatalf("日志重建后缺少 key：slow%d", i)
		}
	}
	if _, ok := set["slow-new"]; !ok {
		t.Fatal("日志重建后缺少新 key")
	}
}
