# noGCMap

`noGCMap` 是一组面向**千万级条目**、**几乎不产生 GC 压力**的并发安全 Map。
它把不利于 GC 的 `map[string]string` 之类结构拆成“基础类型索引 + 连续字节块存储”，
让大规模数据在垃圾回收时不再逐个扫描键值对。

同系列还有 [noGcStaticMap](https://github.com/yudeguang/noGcStaticMap)：性能稍高、内存占用更小，但不支持增删改。
本仓库的版本支持**增、删、改**，代价是性能略低于静态版本、内存占用略高。

- 在泛型可用的场景下也可以直接使用 Go 标准库的 `sync.Map`/`map`，本库的价值在高并发 + 大体量 + 对 GC 停顿敏感的场景。

## 特性

1. 几乎零 GC：键值对存放在按 64K（可配置）对齐的连续 `[]byte` 块中，索引是 `map[uint64]uint`（任意键）或 `map[int]uint`（int 键），不持有业务对象的指针。
2. 并发安全：512 个分片，读写各自加锁，绝大多数操作只碰一个分片；只有超长键值和哈希冲突才走全局小表。
3. 支持 Get / Set / Delete，值为空、键为空都可以。
4. 读路径可做到零分配：`GetTo` 复用调用方缓冲区。
5. 自动回收：覆盖更新与删除都会按“浪费字节占比”触发分片压缩，压缩过程复用已有 chunk 与索引，不产生新的 64K 分配。
6. 无第三方依赖（详见下文 hash 选型）。
7. 代码量小，便于按自己的场景二次修改。

## 安装

模块模式（推荐）：

```bash
go get github.com/yudeguang/noGCMap
```

GOPATH 模式（`GO111MODULE=off`）：

```bash
git clone https://github.com/yudeguang/noGCMap $GOPATH/src/github.com/yudeguang/noGCMap
```

## 快速开始

```go
package main

import (
	"fmt"

	"github.com/yudeguang/noGCMap"
)

func main() {
	m := noGCMap.New() // 可选：noGCMap.New(noGCMap.Config{MultiplesOf64KForchunkSize: 2})

	// 键、值都允许为空
	m.Set([]byte(""), []byte("空键的值"))
	m.SetString("name", "noGCMap")
	m.Set([]byte("binary"), []byte{0x01, 0x02})

	if v, ok := m.GetString("name"); ok {
		fmt.Println("name =", v)
	}

	b, _ := m.Get([]byte("binary"))
	fmt.Println("binary =", b)

	// 需要零分配读取时：复用同一个缓冲区
	buf := make([]byte, 0, 64)
	v, _ := m.GetTo(buf[:0], []byte("name"))
	fmt.Println("GetTo =", string(v))

	m.DeleteString("name")
	_, ok := m.GetString("name")
	fmt.Println("name exists =", ok)
	fmt.Println("len =", m.Len())
}
```

int 键版本：

```go
m := noGCMap.NewInt()
m.SetString(100, "hello")
v, _ := m.GetString(100)
m.Delete(100)
```

## API 速览

`NoGcMapAny`（`[]byte` 键）：

| 方法 | 说明 |
| --- | --- |
| `New(config ...Config) *NoGcMapAny` | 创建实例，非法配置回退默认值 |
| `NewWithError(config ...Config) (*NoGcMapAny, error)` | 创建实例并严格校验配置 |
| `Get(k []byte) ([]byte, bool)` | 读取，返回**副本** |
| `GetTo(dst, k []byte) ([]byte, bool)` | 读取到 `dst`，复用缓冲区时零分配 |
| `GetString(k string) (string, bool)` | 以 string 读取 |
| `Set(k, v []byte)` / `SetString(k, v string)` | 写入或更新，内部复制入参 |
| `Delete(k []byte)` / `DeleteString(k string)` | 删除，分片与 slow 表都会清理 |
| `Len() int` | 键值对数量（并发下为近似值） |
| `KeyToSliceUnSafe(dst [][]byte) [][]byte` | 不加锁收集所有出现过的 key（只多不少） |
| `ChunkSize() int` / `NeedDeletePercent() int` | 查看本实例实际生效的配置 |

`NoGcMapInt`（`int` 键）：`NewInt` / `NewIntWithError` / `GetTo` / `GetString` / `Set` / `SetString` / `Delete` / `Len` / `KeyToSliceUnSafe(dst []int) []int` / `ChunkSize` / `NeedDeletePercent`，语义与上表一致。

## 配置

```go
type Config struct {
	// 已删除记录占该分片已使用字节的百分比达到该值时触发回收。1~100，默认 20。
	// 100 表示只有分片内数据全部被删除后才回收。
	NeedDeketeKVPairPersent int
	// chunk 大小 = MultiplesOf64KForchunkSize * 64 * 1024。1~1000，默认 1（64K）。
	MultiplesOf64KForchunkSize int
}
```

配置是**实例级**的：`New` 之后不会再影响任何已存在的实例，也不会读写包级变量。
需要感知非法配置时用 `NewWithError` / `NewIntWithError`；`New` 保持兼容行为，回退到默认值。

## 行为与语义

- **复制语义**：`Set` 复制键和值（慢表路径也一样），调用方可以立即复用传入的切片；`Get` 返回副本，修改返回值不会影响缓存；`GetTo` 的结果可能复用 `dst` 的底层数组，同样与内部存储无关。
- **落点唯一**：同一个键只可能存在于分片或 slow 表其中一处。值从短变长（超过 chunk 容量或 65535 字节）、从长变短、被覆盖、被删除时，另一侧的旧记录都会被清理，因此不会出现“读到旧值”或“删除后复活”。
- **长键值**：满足 `len(k)+len(v)+5 > chunkSize` 或 `len(k)/len(v) > 65535` 的键值对放入 slow 表，这部分应当是少数；哈希冲突的第二个及以后的键也放这里。
- **回收策略**：按字节判断——某分片被标记删除的数据占该分片已使用字节的比例达到 `NeedDeketeKVPairPersent`，且浪费字节数超过 `max(chunkSize/2, 8KB)`（上限 1MB）时，压缩该分片。判断发生在 `Set`（覆盖更新）与 `Delete` 之后，因此“只写不删”的更新型负载也不会无限膨胀。压缩采用**写时复制**：把存活记录写入全新的 chunk 并整体替换，旧 chunk 不再被改写，交给 GC 回收——这样即使有 goroutine 正在无锁地收集 key，它手里的旧快照也始终可读。
- **Len 语义**：并发写入时不做全局快照，返回近似值；单线程下精确等于键值对数量。
- **批量取 key**：`KeyToSliceUnSafe` 在不加任何 map 锁的前提下收集全部 key，详见下一节。
- **并发安全**：所有导出方法都可并发调用；锁序统一为“分片锁 → 全局锁”，不会出现循环等待。

## 批量取 key：`KeyToSliceUnSafe`

用于部分替代 `range` 的场景：一次把表里出现过的 key 全部取到切片里，然后自己遍历。

```go
keys := m.KeyToSliceUnSafe(nil)
for _, k := range keys {
    // 关键：使用前必须自行判断 key 是否仍然存在
    if v, ok := m.Get(k); ok {
        _ = v
    }
}
```

语义约定：

- **不加锁**：方法内部不获取任何分片锁与全局锁，因此不会阻塞或等待正在进行的 `Set`/`Get`/`Delete`（测试用例把 512 个分片锁和全局锁全部占用后调用它，仍能立即返回）。
- **只多不少**：调用时刻已经存在的 key，一定会出现在结果里。
- **会“多”**：结果可能包含已删除的 key、同一个 key 的历史版本（重复项）。用之前请调用 `Get` 确认是否仍然存在；需要唯一 key 时请自行去重。
- **扫描期间的新写入不做保证**：结果可能包含、也可能不含这期间新写入的 key。
- **不做顺序保证**：结果顺序与分片、chunk 布局有关。
- **返回值是 key 的副本**：修改切片里的 key 不会影响 Map 内部数据。
- **支持复用 `dst`**：`m.KeyToSliceUnSafe(buf[:0])` 可以复用已有切片。

实现方式：每个分片对外发布一份只读视图，视图里同时带着 **chunk 列表**和**每个 chunk 各自的已写入长度**（长度用独立原子变量，写入方先写数据、后发布长度）。扫描只做一次视图读取，再按各 chunk 自己的长度解析：

- 长度与它所属的 chunk 天然是同一份快照，不存在“视图和写入边界分两次发布、中间被压缩打断”的跨版本错配，因此不需要重试，也不会漏 key；
- 压缩采用写时复制：旧视图里的 chunk 与其长度在压缩后都不再变化，正在扫描的 goroutine 可以安全读完；
- 只会“多”不会“少”：扫描期间若有新写入，可能扫到更多记录；已发布区域只追加、不覆盖。

slow 表的 key 通过一份只追加的日志收集。

**为什么不读“删除标记”字节**：`Delete`/覆盖更新会持锁把记录头部的 1 字节标记就地改成 1，而无锁扫描与该写入之间没有 happens-before 边。该标记对输出没有影响（被删除的 key 本来就允许出现在结果里），所以扫描路径**完全不读这一字节**，只用 `keyLen`/`valLen` 定位记录——这条数据竞争因此被彻底消除，`go test -race` 不再报警。

成本：结果里每个 key 都要复制一份，因此耗时为 O(数据总字节) 且必然产生分配。实测（10 万键的小值表，单线程）：收集 65,536 个 key 约 **1.8~2.1 ms**、8.6 MB 分配。建议低频调用（如几秒一次的全量对账），不要放进热路径。

## hash 选型：为什么是标准库 `hash/maphash`

旧版本使用 `github.com/cespare/xxhash`（v1，该模块已被作者标记为 deprecated，推荐 v2）。
本次改为标准库 `hash/maphash`，理由是实测差距远小于依赖成本：

同一台机器（AMD Ryzen 7 PRO 7840U、Go 1.27.3、`-benchtime 3000000x`、0 allocs/op），单位 ns/op：

| 键长度 | `xxhash.Sum64` | `maphash.Bytes` | `fnv1a` | `crc32.ChecksumIEEE` |
| --- | --- | --- | --- | --- |
| 8 B | **2.73** | 4.52 | 4.92 | 8.65 |
| 16 B | **3.12** | 4.39 | - | - |
| 32 B | 6.56 | **4.60** | - | - |
| 64 B | 8.45 | **4.64** | - | - |
| 1 KB | 65.25 | **55.96** | - | - |

结论：

1. 短键（≤16B）xxhash 快约 1.3~1.8 ns，但端到端 `Get` 约 54~85 ns，这点差异只占 2%~3%，用户几乎感知不到；
2. 键长 ≥32B 之后 maphash 反超，且耗时基本与长度无关（AES 加速），对“键长度分布不确定”的场景更稳；
3. maphash 的种子由 `maphash.MakeSeed()` 为每个实例随机生成，天然抗哈希碰撞攻击（HashDoS），旧实现是固定种子算法；
4. 移除依赖后本库**不再有任何第三方依赖**，`go.sum` 为空，供应链和维护成本都更低。

因此默认选择 `hash/maphash`。若你的场景是“键极短、且哈希占比极高”的极限压测，可以自行换回 xxhash：把 `NoGcMapAny.seed` 换成 `uint64` 种子并在 `Set`/`Get`/`Delete` 里改用 `xxhash.Sum64` 即可，其余逻辑不需要动。

## 性能

同一台机器的实测（16 线程 `RunParallel`，200 万次，10 万键，三次运行取中位数）：

| 操作 | 耗时 | 分配 |
| --- | --- | --- |
| `NoGcMapAny.Set` | 19.0 ns/op | 0 allocs/op |
| `NoGcMapAny.Get`（返回副本） | 7.9 ns/op | 1 alloc/op（15 B/op） |
| `NoGcMapAny.GetTo`（复用缓冲） | 6.0 ns/op | **0 allocs/op** |
| `NoGcMapAny.SetString` | 14.6 ns/op | 0 allocs/op |
| `NoGcMapInt.Set` | 17.4 ns/op | 0 allocs/op |
| `NoGcMapInt.Get` | 5.9 ns/op | 1 alloc/op |
| `NoGcMapInt.GetTo` | 4.1 ns/op | 0 allocs/op |
| 标准 `map` + `RWMutex` Get（参照） | 20.4 ns/op | 0 allocs/op |
| 标准 `map` + `Mutex` Set（参照） | 103.0 ns/op | 5 B/op |

单线程对照（同一组 10 万键、值 `valueN`）：

| 操作 | 旧实现 | 新实现 |
| --- | --- | --- |
| `Get` | 90~94 ns/op，1 alloc | 77~85 ns/op，1 alloc |
| `GetTo` | 无此接口 | 54 ns/op，**0 alloc** |
| `Set`（覆盖更新） | 123 ns/op | 98.8 ns/op，37 B/op |

旧实现的 `Set` 不回收更新产生的垃圾记录，所以“看起来更快”；修复回收后单线程 `Set` 仍快于旧实现，是因为更新路径不再复制旧值。表中 37 B/op 来自压缩的写时复制（为支持无锁 key 扫描，压缩不再就地覆写旧 chunk）。

复现命令：

```bash
go test -run XXX -bench . -benchmem ./...
```

## 测试

```bash
go test ./...            # 全部用例，约 1 秒
go test -short ./...     # 跳过规模较大的压力测试
go test -race ./...      # 竞态检测：当前实现已无数据竞争
```

本机如果使用较旧的 MinGW（如 gcc 7.3），`go test -race` 可能因 race 运行库 DLL 冲突报 `0xc0000139`，
可以改成静态链接绕开运行库依赖：

```bash
go test -race -ldflags "-linkmode external -extldflags -static" ./...
```

覆盖点包括：基本读写删、空键/空值、短值与长值双向迁移、删除不复活、切片别名隔离、实例配置隔离、配置校验、仅更新也触发回收、压缩后数据完整且可继续写入、`GetTo` 零分配、并发读写删、与标准 map 的随机差分比对（20 万次操作，含负键与长值）。

## 与旧版本的差异

见 [CHANGELOG.md](CHANGELOG.md)。简要：

- 修复“包级 `chunkSize` 被 `Config` 改写导致其它实例读取越界 panic”；
- 修复“长值/哈希冲突的键在分片与慢表中各存一份，导致读到旧值、删除后复活、`Len` 重复计数”；
- 修复“慢表路径保存/返回调用方切片”造成的别名污染；
- 修复“覆盖更新产生的垃圾记录只有在 `Delete` 时才回收”的内存膨胀；
- 新增 `GetTo`，并把 `int` 版本的定长编解码从 `bytes.Buffer` 换成 `binary.BigEndian`，读路径零分配；
- 移除 `github.com/cespare/xxhash` 依赖，改用标准库 `hash/maphash`；
- 测试与基准重写，`go test ./...` 在 module 与 GOPATH 模式下均可直接运行；
- 无锁 key 扫描改为“每 chunk 原子写入长度 + 单次视图读取”，彻底消除“旧视图配新边界”导致的漏 key。

## 限制

- 单条键或值最大 65535 字节，超出部分进入 slow 表（仍可正常读写，但会有 map 的 GC 成本）。
- `Len()` 在并发写入时是近似值。
- 回收是分片级、一次性完成的：单个分片压缩期间会持有该分片的写锁，压缩成本与该分片存活数据量成正比。
- 压缩改为写时复制后，每次压缩会分配与存活数据等量的 chunk；压缩阈值是“浪费 ≥ max(chunkSize/2, 8KB)”，因此写放大和额外分配是有界的。
- `KeyToSliceUnSafe` 的结果含重复与已删除 key，且必须自行 `Get` 校验；它适合低频全量场景，不适合高频热路径。
- 无锁扫描不阻塞写入；压缩是写时复制，扫描期间撞上压缩时读到的仍是完整可用的旧视图（可能包含更多已删除的 key，只会偏多）。
- 不做持久化、不做过期淘汰；需要 TTL 请在业务层或外层再包一层。