# 变更记录

## 2026-11-10 修复：降级读取的跨版本错配（漏 key）

### 问题

`appendKeysOfBucket` 在 16 次版本重试耗尽后，会执行一次**无一致性校验**的降级读取：
先 `view.Load()`、再 `dataEnd.Load()`。由于 `compact` 内是两次独立的原子发布
（先 `atomic.StoreUint64(&b.dataEnd, pos)`，再 `publishLocked`），极端时序下可能拿到
“旧视图 + 新（更小）dataEnd”的组合，从而把旧视图截断解析，漏掉真实存在的 key。

实验中直接构造该组合（2 万条数据、删一半、逐分片强制压缩）：

```
自洽组合(旧视图+旧end) 覆盖=20000/20000
撕裂组合(旧视图+新end) 存活键缺失=4898/10000，全部键缺失=9997/20000
```

可达性极低（要连续 16 轮重试都撞上压缩），但逻辑成立。另外，单纯对调两行加载顺序
**不能**解决：两次独立发布的窗口只是从“落在两次加载之间”搬到“夹住两次加载”，依旧可能错配。

### 修复

不再依赖“视图 + 全局写入边界”这种跨对象配对，改为**每个 chunk 自带原子写入长度**：

- `chunksView` / `chunksViewInt` 由 `chunks` 扩展为 `chunks + lens`（每 chunk 一个 `*uint64`）；
- 写入方先追加数据，再 `atomic.StoreUint64(lens[i], len(chunk))` 发布长度；
- 扫描方只读一次视图，然后对每个 chunk 按它自己的长度解析，长度与数据同属一份快照；
- 压缩（写时复制）整体替换为新 chunk + 新长度，旧视图的 chunk 与长度不再变化，可安全读完；
- 删除原先的 `dataEnd` 字段、版本重试循环和“极端情况降级读取”，`compactVersion` 仅保留用于观测/测试。

这样“漏 key”的组合在结构上不可能出现：不存在需要配对的两个独立发布。

### 验证

- `go test ./...`（GOPATH 与 module 两种模式）全部通过，测试数量仍为 47 个；
- `go test -race -ldflags "-linkmode external -extldflags -static" ./...` 通过，无 DATA RACE；
- 在既有用例中增加了“压缩前捕获旧视图、压缩后用旧视图解析”的断言（any 与 int 各一处），
  验证旧视图在压缩后仍完整可读；
- 基准无回归：`Set` 约 20~22 ns/op、`Get` 约 9~13 ns/op（机器噪声内），`GetTo` 仍 0 分配；
  扫描 65,536 个 key 约 1.8~2.1 ms。

## 2026-11-10 修复：无锁扫描与删除标记的数据竞争

### 问题

`go test -race` 在 `TestAnyKeyToSliceUnSafeConcurrent` 上报出 DATA RACE：

```
Write at ... by goroutine 24:
  (*NoGcMapAny).markAsDelete()  any.go:497   // chunk[offset] = 1
Previous read at ... by goroutine 7:
  parseChunkKeys()               any.go:379   // c[offset] == 0
```

`markAsDelete` 在持有分片写锁时就地改写已发布的字节，而无锁扫描既不持锁、也没有原子操作覆盖该地址，两者之间不存在 happens-before 边，按 Go 内存模型属于数据竞争。实际后果良性（读到 0 或 1 都会把 key 收进结果），但只要 CI 有 `-race` 门禁就会失败。

### 修复

扫描路径**完全不读这一字节**。`parseChunkKeys` / `parseChunkKeysInt` 只用记录头部的 `keyLen` / `valLen` 定位下一条记录，并按 `dataEnd` 边界停止；空 key 记录（`keyLen == 0`）只收一次，用来吸收尾部未写区域的零字节。删除标记对输出没有影响，不读它就从根上去掉了这条竞争，同时保持“只多不少”的语义。

顺带删除不再需要的 `scanZeroRunLimit` 常量与 `isAllZero` 辅助函数。

### 验证

- 修复前：`go test -race` 在 `TestAnyKeyToSliceUnSafeConcurrent` 稳定复现上述 DATA RACE；
- 修复后：`go test -race ./...` 全量通过，无任何 DATA RACE 报告；
- 非 race 模式全量用例、`gofmt`、`go vet` 均通过；
- 扫描成本不变（6.5 万 key 约 1.7~1.9 ms），`Set`/`Get` 基准无变化。

注：本机 MinGW gcc 7.3 的 race 运行库存在 DLL 冲突（`0xc0000139`），可用
`-ldflags "-linkmode external -extldflags -static"` 静态链接后正常运行，上述 race 结论即在该方式下取得。

## 2026-11-10 新增：不加锁批量取 key（KeyToSliceUnSafe）

### 新增方法

`NoGcMapAny.KeyToSliceUnSafe(dst [][]byte) [][]byte` 与 `NoGcMapInt.KeyToSliceUnSafe(dst []int) []int`：
在不获取任何 map 锁的前提下，把表中出现过的 key 追加到切片返回。另提供包内别名 `keyToSliceUnSafe`。

语义：

- 调用时刻已经存在的 key 一定出现在结果中（只多不少）；
- 结果可能包含已删除的 key、同一个 key 的多个历史版本；
- 扫描期间新写入的 key 不做保证；
- 返回的 key 是副本，支持复用 `dst`；
- 使用前必须自行 `Get` 判断 key 是否仍然存在。

### 为支持该语义所做的结构调整

1. **分片增加只读视图与写入边界**：`bucket.view` 保存 `chunks` 的不可变快照（只复制 slice header），`bucket.dataEnd` 原子发布已写入数据的末尾。扫描只读这两者，不碰索引、不碰锁。
2. **压缩改为写时复制**：`compact` 不再就地复用 chunk 空间，而是把存活记录写入全新的 chunk 后整体替换。这样扫描持有的旧视图不会被就地覆写，仍能安全读完。同时用 `compactVersion`（压缩期间为奇数）与扫描协调，扫描撞上压缩时重试，避免读到“视图/边界”不匹配的中间状态。
3. **slow 表 key 日志**：slow 表（超长键值、哈希冲突）的 key 记录在一份只追加的日志里，扫描时直接读日志，同样不需要加锁访问 map；日志按 `2*len(slow)+1024` 的阈值重建，不会无限增长。
4. **回收阈值调整**：由于压缩改为写时复制，压缩频率过高会带来额外分配，因此触发条件从“浪费 ≥ max(chunkSize/8, 2KB)”改为“浪费 ≥ max(chunkSize/2, 8KB)（上限 1MB）且占比 ≥ NeedDeketeKVPairPersent”。

### 代价与实测

- 单线程覆盖更新基准：`Set` 98.8 ns/op、37 B/op（此前为 100~109 ns/op、19 B/op），额外分配来自压缩的写时复制；多线程基准基本不变（`Set` 19.0 ns/op、0 allocs/op）。
- 收集 65,536 个 key（10 万键的小值表）：约 1.75 ms、8.6 MB、65,562 次分配（每个 key 都要复制一份）。
- 并发验证：单分片 7,834 轮压缩期间完成 9,421 次扫描，预先写入的 200 个 key 一次未缺；把 512 个分片锁与全局锁全部占用后调用该方法仍能立即返回（证明它不依赖 map 锁）。

## 2026-11-10 修复与重构

本次针对一次只读缺陷审查中确认的问题做修复。审查基线是提交 `9b5ad42` 的工作区版本，
下文“原位置”指修复前的 `any.go` / `int.go`。

### 一、高危（可崩溃 / 脏读 / 数据被改写）

**1. 包级 `chunkSize` 被 `Config` 改写，导致其它实例读取越界 panic**
原位置：`any.go:14`（`var chunkSize = 64 * 1024`）、`any.go:45-53`（`New` 覆盖包级变量）、`any.go:205-206/225-226/279-283`（读取时用当时的全局值反算偏移）；`int.go:29-37` 等。
现象：先创建一个 64K 实例写入足量数据，再创建任意一个 128K 实例，前一个实例的读取会按 128K 解释旧的线性偏移；实测 70,000 次读取中 15,216 次 `slice bounds out of range`。
修复：`chunkSize`、`needDeletePercent` 改为 `NoGcMapAny` / `NoGcMapInt` 的实例字段，所有读取/写入/标记删除/压缩路径都使用实例字段；删除包级可变变量。
配套：新增 `ChunkSize()` / `NeedDeletePercent()` 便于校验；新增 `NewWithError` / `NewIntWithError` 对配置做严格校验，`New` / `NewInt` 保持兼容（非法值回退默认值）。
回归用例：`TestAnyConfigIsolation`、`TestIntConfigIsolation`、`TestAnyConfigValidation`。

**2. 同一个键可能同时存在于分片与 slow 表，导致读到旧值、删除后复活、`Len` 双计**
原位置：`any.go:98-105`（慢表写入不清理分片）、`any.go:68-82`（读取优先分片）、`any.go:149-161`（删除命中分片即跳过慢表）；`int.go:80-87`、`int.go:51-65`、`int.go:125-137`。
现象：`Set("k1","v-short")` 后 `Set("k1", 70000B)`，`Get` 返回旧的短值且 `Len()==2`；随后 `Delete("k1")`，`Get` 又把长值取回来（删除后复活）。
修复：写入时保证“落点唯一”——分片写入后清理 slow 表中的同键旧值，慢表写入前清理分片中的同键旧记录（先分片锁后全局锁，锁序一致）；删除时两侧都清理；`Get` 仍以分片优先，但只有两侧都不存在才返回 `false`。
新增分片级 `slowItemNum` 计数器，让绝大多数写操作无需触碰全局锁。
回归用例：`TestAnyLongKVAndMigration`、`TestIntLongKVAndMigration`、`TestAnyRandomDifferential`、`TestIntRandomDifferential`（含长短值迁移与 slow 计数校验）。

**3. slow 表路径的切片别名**
原位置：`any.go:102`（保存调用方切片）、`any.go:78`（返回内部切片）；`int.go:84`、`int.go:61`。
现象：改传入的 value 会改掉缓存内容；改 `Get` 返回值会污染后续读取；而分片路径是复制的，两条路径语义不一致。
修复：`Set` 慢表写入时复制值，`Get`/`GetTo` 返回副本；分片路径同样只返回副本。
回归用例：`TestAnySliceAliasing`、`TestIntSliceAliasing`。

### 二、中危

**4. 覆盖更新产生的垃圾记录只在 `Delete` 时回收**
原位置：`any.go:117`（更新时只标记删除并计数）、`any.go:162-163`（回收判断只写在 `Delete` 内）；`int.go:99`、`int.go:139`。
现象：单键覆盖 20,000 次后，存活 1 条却占用 208,890 字节、4 个 chunk，`deleteItemNum=19,999` 从不回收。
修复：回收判断抽成 `maybeCompact`，`Set`（覆盖更新）与 `Delete` 之后都会调用；阈值改为按“被浪费字节 / 已使用字节”计算（默认 20%），并增加最小浪费字节（`max(chunkSize/8, 2KB)`，上限 256KB）避免过度压缩。
配套：压缩改为原地复用 chunk 空间、原地清空并重建索引，临时缓冲来自 `sync.Pool`，压缩本身不再产生新的 64K 分配。
回归用例：`TestAnyUpdateTriggersCompaction`、`TestIntUpdateTriggersCompaction`、`TestAnyCompactionKeepsData`、`TestIntCompactionKeepsData`。

**5. `Get` 每次分配，与“零 GC”定位矛盾**
原位置：`any.go:241-242`（`make + append` 复制值）、`int.go:213-214`、`int.go:298-303`（`bytesToInt` 每次 `bytes.NewBuffer`）、`int.go:291-296`（`intToBytes` 每次 `bytes.NewBuffer`）。
修复：新增 `GetTo(dst, k)`，命中时把值追加到调用方缓冲区；`int` 版改用 `binary.BigEndian.Uint64` 与定长 `[8]byte`，读写路径均不再使用 `bytes.Buffer`。
实测：`NoGcMapInt.Get` 由 3 allocs/op 降为 1 alloc/op（仅返回值副本），`GetTo` 为 0 allocs/op。
回归用例：`TestAnyGetToZeroAlloc`、`TestIntGetToZeroAlloc`。

**6. 基准文件依赖未声明，module 模式 `go test ./...` 失败**
原位置：`any_benchmark_test.go:6` 引入 `github.com/VictoriaMetrics/fastcache`，而 `go.mod` / `go.sum` 未声明。
修复：基准文件改为只使用标准库，提供 `BenchmarkAnySet/Get/GetTo/SetString`、`BenchmarkIntSet/Get/GetTo` 以及与标准 `map` 的对照；不再引入第三方依赖。

### 三、低危

**7. `BenchmarkSet` 实际测的是“值相同直接返回”的空转**
修复：所有写基准每轮改写值的一个字节，确保进入真实写入路径；文件头注释说明这一约定。

**8. 测试在子 goroutine 中调用 `t.Fatalf`**
原位置：`any_test.go:68,71,116,136,139`；`int_test.go:64,67,104,124,127`（`go vet` 共 10 处告警）。
修复：goroutine 内只使用 `t.Errorf`（并发安全，且不会 `Goexit` 跳过 `wg.Done`），等待结束后在主 goroutine 判断结果；测试重写后 `go vet` 不再有告警。

**9. `int_test` 断言期望与失败信息不一致**
原位置：`int_test.go:91-92`（断言 `max+2`，信息里写 `max+3`）。测试重写后该问题不再存在。

**10. 边界与语义问题**
- 删除阈值的除零：原 `any.go:163` 在 `len(index)==0` 时依赖浮点除零得到 `+Inf`；新实现改为整数比较并显式判断 `dataBeginPos == 0`。
- 阈值为 100 时无法关闭回收：原实现按“删除条数 / 存活条数”计算，更新可使比例远超 100%；新实现按字节比例计算，100 表示只有全部数据被删除后才回收，语义明确。
- 配置越界静默忽略：新增 `NewWithError` / `NewIntWithError` 返回错误；`New` 保持兼容。
- `abs()` 对 `math.MinInt64` 溢出隐患：int 版分片计算改为 `bucketIndexOf`，只在 `k % bucketsNum` 结果上取绝对值。
- chunk 槽位只增不减：压缩后 `chunks` 截断到实际使用的长度，空槽不再累积。
- 读取/标记删除增加边界校验，遇到损坏偏移返回 `false` 而不是 panic。

**11. README 与真实 API 不符**
原 README 示例来自 noGcStaticMap（`NewDefault`、`SetFinished` 在本包不存在）。README 已重写为可直接运行的示例，并补充 `Example` 测试（`example_test.go`）保证文档代码可编译、输出可校验。

**12. gofmt 未通过**
修复后 `gofmt -l .` 无输出。

### 四、hash 实现调整

移除 `github.com/cespare/xxhash`（v1 已被上游标记 deprecated），改用标准库 `hash/maphash`。
同一台机器实测（ns/op，0 allocs）：8B 键 `xxhash` 2.73 对 `maphash` 4.52；16B 3.12 对 4.39；32B 6.56 对 4.60；64B 8.45 对 4.64；1KB 65.25 对 55.96。
短键上 xxhash 领先 1.3~1.8 ns（占端到端 `Get` 的 2%~3%），键长 ≥32B 后 maphash 反超且耗时与长度基本无关；再加上“每实例随机种子”的抗 HashDoS 能力与零依赖收益，选择 `hash/maphash`。取舍细节与换回 xxhash 的方式写在 README 的“hash 选型”一节。

### 五、性能

单线程（10 万键，覆盖更新）：

| 操作 | 旧实现 | 新实现 |
| --- | --- | --- |
| `Get` | 90~94 ns，1 alloc | 77~85 ns，1 alloc |
| `GetTo` | - | 54 ns，0 alloc |
| `Set` | 123 ns | 100~109 ns，0 alloc |

`go.mod` 现无第三方依赖，`go.sum` 为空；`go test ./...` 在 module 与 `GO111MODULE=off` 两种模式下均可运行。