// Copyright 2020 rateLimit Author(https://github.com/yudeguang/noGCMap). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/yudeguang/noGCMap.
package noGCMap

import (
	"fmt"
)

func ExampleNew() {
	m := New()
	m.SetString("hello", "world")
	m.Set([]byte("binary"), []byte{0x01, 0x02})

	v, _ := m.GetString("hello")
	fmt.Println(v)

	b, _ := m.Get([]byte("binary"))
	fmt.Println(b)

	m.DeleteString("hello")
	_, exist := m.GetString("hello")
	fmt.Println(exist)
	fmt.Println(m.Len())
	// Output:
	// world
	// [1 2]
	// false
	// 1
}

func ExampleNewWithError() {
	// 需要严格校验配置时使用 NewWithError
	_, err := NewWithError(Config{MultiplesOf64KForchunkSize: 1001})
	fmt.Println(err != nil)

	m, err := NewWithError(Config{MultiplesOf64KForchunkSize: 2, NeedDeketeKVPairPersent: 30})
	fmt.Println(err, m.ChunkSize(), m.NeedDeletePercent())
	// Output:
	// true
	// <nil> 131072 30
}

func ExampleNoGcMapAny_GetTo() {
	m := New()
	m.SetString("k", "reuse-buffer")

	// 复用同一个缓冲区读取，避免每次读取都分配内存
	buf := make([]byte, 0, 64)
	v, _ := m.GetTo(buf[:0], []byte("k"))
	fmt.Println(string(v))
	// Output:
	// reuse-buffer
}

func ExampleNoGcMapAny_KeyToSliceUnSafe() {
	m := New()
	m.SetString("a", "1")
	m.SetString("b", "2")

	// 不加锁地取出所有出现过的 key：只可能多（含已删除的、重复的），不会少
	keys := m.KeyToSliceUnSafe(nil)
	alive := 0
	for _, k := range keys {
		// 每个 key 用之前必须自行判断是否仍然存在
		if _, exist := m.GetString(string(k)); exist {
			alive++
		}
	}
	fmt.Println(alive)
	// Output:
	// 2
}
