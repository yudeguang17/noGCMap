// Copyright 2020 rateLimit Author(https://github.com/yudeguang/noGCMap). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/yudeguang/noGCMap.
package noGCMap

import (
	"strconv"
	"sync"
	"testing"
)

// 说明：
// 1. 所有基准都使用“每轮改写一个字节”的方式构造新值，
//    避免命中 Set 中“值未变化则直接返回”的快捷路径，否则测到的是空转而不是写入。
// 2. 本文件不引入任何第三方依赖，保证 module 模式下 go test ./... 可以直接跑通。
//    如需与 fastcache 等实现对比，请在单独的测试模块中进行，不要给库本身引入依赖。

const benchItems = 1 << 16

func benchKeys() [][]byte {
	keys := make([][]byte, benchItems)
	for i := range keys {
		keys[i] = []byte(strconv.Itoa(i))
	}
	return keys
}

func BenchmarkAnySet(b *testing.B) {
	keys := benchKeys()
	m := New()
	v := []byte("value")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		val := append([]byte(nil), v...)
		for pb.Next() {
			val[0]++
			m.Set(keys[i%benchItems], val)
			i++
		}
	})
}

func BenchmarkAnyGet(b *testing.B) {
	keys := benchKeys()
	m := New()
	for i, k := range keys {
		m.Set(k, []byte("value"+strconv.Itoa(i)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Get(keys[i%benchItems])
			i++
		}
	})
}

func BenchmarkAnyGetTo(b *testing.B) {
	keys := benchKeys()
	m := New()
	for i, k := range keys {
		m.Set(k, []byte("value"+strconv.Itoa(i)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		buf := make([]byte, 0, 64)
		for pb.Next() {
			v, _ := m.GetTo(buf[:0], keys[i%benchItems])
			buf = v
			i++
		}
	})
}

func BenchmarkAnySetString(b *testing.B) {
	keys := make([]string, benchItems)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	m := New()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.SetString(keys[i%benchItems], "value")
			i++
		}
	})
}

func BenchmarkIntSet(b *testing.B) {
	m := NewInt()
	v := []byte("value")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		val := append([]byte(nil), v...)
		for pb.Next() {
			val[0]++
			m.Set(i%benchItems, val)
			i++
		}
	})
}

func BenchmarkIntGet(b *testing.B) {
	m := NewInt()
	for i := 0; i < benchItems; i++ {
		m.Set(i, []byte("value"+strconv.Itoa(i)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Get(i % benchItems)
			i++
		}
	})
}

func BenchmarkIntGetTo(b *testing.B) {
	m := NewInt()
	for i := 0; i < benchItems; i++ {
		m.Set(i, []byte("value"+strconv.Itoa(i)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		buf := make([]byte, 0, 64)
		for pb.Next() {
			v, _ := m.GetTo(buf[:0], i%benchItems)
			buf = v
			i++
		}
	})
}

// BenchmarkStdMapGet 作为参照：标准 map + RWMutex。
func BenchmarkStdMapGet(b *testing.B) {
	keys := benchKeys()
	m := make(map[string][]byte, benchItems)
	for i, k := range keys {
		m[string(k)] = []byte("value" + strconv.Itoa(i))
	}
	var mu sync.RWMutex
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			mu.RLock()
			_ = m[string(keys[i%benchItems])]
			mu.RUnlock()
			i++
		}
	})
}

// BenchmarkStdMapSet 作为参照：标准 map + Mutex。
func BenchmarkStdMapSet(b *testing.B) {
	keys := benchKeys()
	m := make(map[string][]byte, benchItems)
	var mu sync.Mutex
	v := []byte("value")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		val := append([]byte(nil), v...)
		for pb.Next() {
			val[0]++
			mu.Lock()
			m[string(keys[i%benchItems])] = val
			mu.Unlock()
			i++
		}
	})
}

// BenchmarkKeyToSliceUnSafe 测量不加锁收集全部 key 的成本（每轮都会分配新切片，故用较小的 benchtime）。
func BenchmarkKeyToSliceUnSafe(b *testing.B) {
	keys := benchKeys()
	m := New()
	for _, k := range keys {
		m.Set(k, []byte("v"))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.KeyToSliceUnSafe(nil)
	}
}
