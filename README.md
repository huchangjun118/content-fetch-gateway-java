# Content Fetch Gateway

面向多来源的内容获取网关：从多个内容提供方拉取**内容寻址**（content-addressed，
地址即内容的 SHA-256）的数据，对上层提供稳定且一致的读取。所有来源与异常场景
均通过本地测试替身（`FakeContentSource`）模拟，不依赖外部服务或真实凭据。

## 架构

```
GET /gateway/content/{id}        GatewayController
        │                        GatewayService
        ▼
 FetchCoordinator ── 单飞合并（single-flight）、超时、取消
        │
        ├─ SourceHealthRegistry  可用性评估 / 隔离 / 探测恢复
        ├─ ContentCache          已校验分块 + 完整内容，TTL 过期，读取时重校验
        └─ ContentSource[]       按 priority 排序的多来源（测试中为 FakeContentSource）
```

## 一致性语义

- **内容寻址即信任根**：`ContentId` 是完整内容的 SHA-256。清单（manifest）声明
  每个分块的哈希，清单本身由内容地址背书。
- **逐块校验**：来源返回的每个分块都先对清单校验，通过后才进入缓存或参与组装；
  未通过即视为来源损坏（`CorruptContentException`）。
- **整体校验**：组装后的完整内容必须重新哈希等于内容地址，才会返回给调用方并
  发布到缓存。**未校验的片段永远不会跨来源拼接**；只有各自通过清单校验的分块
  才允许复用，因此无论分块来自哪里，最终结果由内容地址唯一确定。
- **首个完整校验副本获胜**：按来源优先级依次尝试，返回第一份通过全部校验的
  完整内容；损坏或抖动来源自动回退到下一来源。
- **断点续传**：中断后从缓存中已校验的位置继续；已校验分块不重复下载、不重复
  落盘。未完成的获取只可能留下已校验的分块，**绝不会留下被当作可用结果的半份
  内容**（完整内容只有整体校验通过后才发布）。
- **缓存一致性**：缓存命中前重新校验哈希，损坏条目即弃即取（自愈），不影响
  正在进行的读取；条目按 TTL 过期后重新从来源拉取，命中不会返回与来源当前
  内容不一致的结果。
- **并发合并**：同一内容的并发读取共享同一次获取（single-flight），重复请求
  不穿透到来源；缓存命中直接返回，慢来源的在途获取不会拖垮已可用的读取。
- **来源降级**：连续失败达到阈值或返回损坏内容（按来源可信度加权，惩罚更重）
  的来源被隔离一段时间；隔离期满后先探测（`probe()`），成功才重新参与调度，
  失败则延长隔离。
- **取消与超时**：单次来源尝试受 `sourceTimeout` 约束，整体读取受
  `fetchTimeout` 约束；调用方取消只会 detach 自己，不影响共享同一获取的其他
  读取方。

## 配置项（前缀 `gateway`）

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `gateway.fetch-timeout` | 30s | 单次完整内容获取的最大耗时，超时以 `TimeoutException` 失败 |
| `gateway.source-timeout` | 5s | 单个来源尝试的最大耗时，超时后回退下一来源 |
| `gateway.max-consecutive-failures` | 3 | 连续失败（损坏按可信度加权）达到该值即隔离来源 |
| `gateway.quarantine-period` | 30s | 来源隔离时长，期满后探测成功才恢复调度 |
| `gateway.cache-ttl` | 5m | 缓存条目（分块与完整内容）的存活时间 |

## 来源接口

```java
public interface ContentSource {
    String id();                 // 稳定标识
    int priority();              // 越小越优先
    double trust();              // 可信度 [0,1]，影响损坏惩罚权重
    ContentManifest manifest(ContentId id);
    Chunk fetchChunk(ContentId id, int chunkIndex);
    boolean probe();             // 隔离期满后的存活探测
}
```

## 测试

`src/test/java/.../gateway/` 下的自动化测试全部基于本地替身：

- `MultiSourceFetchTest` — 来源损坏回退、来源抖动恢复
- `ResumeFetchTest` — 中断续传，已校验分块不重复下载/落盘
- `ConcurrencyTest` — 并发读取合并为单次获取，慢来源不拖垮缓存命中
- `SourceHealthTest` — 失败计数、隔离、探测恢复、损坏加重惩罚
- `CacheTest` — TTL 过期重取、缓存内容/分块损坏自愈
- `CancellationTimeoutTest` — 来源超时回退、整体超时、取消不留半份内容

运行：

```bash
mvn test
```
