# AGENTS.md — 通用批处理流水线框架（java-batch-pipeline）

> 本文件面向**任何 AI 编程模型**（不绑定任何特定产品）。目标：让模型在读完本文件后，能准确理解本框架的设计与 API，并据此把"大批量数据 + 分流落盘"类的需求落地为本项目的代码。
>
> 更权威的实现细节见 [README.md](./README.md) 与 [DESIGN.md](./DESIGN.md)；**当本文件与代码或它们冲突时，以代码为准**。本文件是"上手地图"，不是规范的替代品。

---

## 0. 一句话定位

本框架解决一类固定形态的问题：**有一个明确的、规模可能很大的输入（文件 / 数据库游标 / 任意可迭代源）→ 每 N 条打包成一批 → 多线程并发调用某个批量接口（HTTP / RPC / 数据库）→ 根据每条记录的处理结果，分流写入一个或多个输出文件（含一个错误文件）**。

它的价值不在"业务逻辑"（那由你写），而在**把并发、背压、分片落盘、合并、异常降级、路由兜底这些脏活统一收口**。你只需要提供三样东西：怎么读、怎么处理一批、一行长什么样。

---

## 1. 适用 / 不适用

**适用**：输入有终点、单条处理慢、希望用多线程加速、且结果需要按状态/成败分流到不同文件。典型例子：批量推送订单、批量查询状态、批量调用风控、批量导入清洗。

**不适用**：流式无终点、需要 Exactly-Once 事务、需要跨进程断点续跑（本框架有分片但未内置断点续跑）、单条处理极快且不需要并发。

---

## 2. 项目结构与构建

标准 Maven 结构（零第三方运行时依赖，纯 JDK 即可编译运行，目标字节码 Java 8）：

```
java-batch-pipeline/
├── pom.xml                       ← Maven 构建（release 8，编译期 -Xlint:all 零警告）
├── src/main/java/com/example/
│   ├── batch/                   ← 通用内核（框架本体，不要改，除非修复 bug）
│   │   ├── RecordReader.java        读取接口（SPI）
│   │   ├── RecordReaders.java       Iterator / read() 形态适配器 + 表头表尾组合器
│   │   ├── BatchProcessor.java      批处理接口（SPI，通常调 HTTP/RPC）
│   │   ├── RoutingProcessor.java    可选 SPI：按结果分流到不同通道
│   │   ├── ItemResult.java          单条结果模型
│   │   ├── RecordFormatter.java     输出行格式（单方法）
│   │   ├── OutputChannel.java       通道定义（key + 输出目标 + formatter）
│   │   ├── ChannelTarget.java       输出目标抽象（SPI）：open() 暂存 / commit() 提交
│   │   ├── ChannelTargets.java      输出目标工厂：ChannelTargets.file(File)
│   │   ├── ChannelRouter.java       路由 + 未注册兜底
│   │   ├── PipelineConfig.java      配置 + Builder（通道表 + 校验 + 可选 groupBy 分组）
│   │   ├── GroupKeyExtractor.java   分组键提取器（SPI，从记录取分组键，不抛受检异常）
│   │   ├── BatchingStrategy.java    「如何成批」策略接口（默认按数量 / 分组按 key）
│   │   ├── CountBatchingStrategy.java  默认攒批策略：按到达顺序 + batchSize 切批
│   │   ├── GroupBatchingStrategy.java  内置分组策略：按 key 入桶，攒满即切成 batchSize 大小的单组批次
│   │   ├── BatchResult.java         运行结果只读快照（run() 的返回值）
│   │   ├── BatchPipelines.java      一行式入口：BatchPipelines.run(config)
│   │   ├── BatchPipeline.java       内核编排（生产者/背压/合并/停机）
│   │   ├── PartWriter.java / OutputMerger.java / BatchTask.java
│   │   ├── BatchFailedException.java / PipelineExecutionException.java
│   │   └── PipelineLog.java / PipelineStats.java
│   └── demo/                    ← 示例业务实现（替换成你的真实实现）
│       ├── DemoLineReader.java / DemoHttpProcessor.java / DemoLineFormatter.java
│       ├── DemoMain.java（双输出）/ DemoOrderMain.java + DemoOrder*（多通道）
│       ├── DemoGroupRecord.java / DemoGroupReader.java / DemoGroupProcessor.java
│       │   / DemoGroupFormatter.java / DemoGroupMain.java（内置分组：按 key 字段分组，每满一批同组记录才调下游）
└── run.sh                       ← 一键 Maven 编译并跑全部示例
```

构建与运行（Maven 已就位）：

```bash
mvn compile                                   # 编译到 target/classes
java -cp target/classes com.example.demo.DemoMain            # 跑主 Demo（双输出）
java -cp target/classes com.example.demo.DemoOrderMain      # 跑多通道 Demo
java -cp target/classes com.example.demo.DemoGroupMain      # 跑内置分组 Demo（按 key 分组）
# 或：./run.sh（自动 mvn compile 后依次跑上述三个 Demo）
```

---

## 3. 核心模型（必须遵守的事实）

| 概念 | 含义 |
|---|---|
| 记录 `T` | 一条原始输入（行、订单字符串、对象……）。框架不解读它。 |
| 处理结果 `ItemResult<P>` | 一条记录被处理后的结果。`success(code, payload)` 或 `failed(code, message)`。`P` 是附加内容类型（如服务端逐条返回的字段）。 |
| 通道 `OutputChannel` | 一类结果 → 一个输出目标 + 一个行格式。**通道不在业务代码里 new**，由配置声明。 |
| 输出目标 `ChannelTarget` | 通道结果的最终归宿：落文件、上传对象存储、写标准输出……落本地文件用 `ChannelTargets.file(File)`。 |
| 路由 | 每条记录被"解析"成一个**已注册**的通道 key，写进对应通道的输出目标。 |

### 3.1 输出通道的两种声明方式

- **普通业务通道**：`.channel("paid", target, formatter[, createEmpty])` —— key 任意（ASCII），成功记录可路由到这里。
- **错误通道（兜底安全网）**：`.errorChannel(target, formatter[, createEmpty])` —— key 固定为 `"error"`，由方法内部设定，**调用方不能改名**。

**错误通道是构建期强制项**：`build()` 时若未声明 `.errorChannel(...)`，直接抛 `IllegalArgumentException`。所以任何装配都必须包含它。原因：记录级失败、批次级失败、未注册路由全部落这里；若缺失，第一次失败就会让整条流水线崩溃。

保留 key 常量：`OutputChannel.SUCCESS = "success"`、`OutputChannel.ERROR = "error"`。

### 3.2 路由规则（核心，务必牢记）

1. **失败的记录一律进错误通道**。`RoutingProcessor.channelOf(...)` 只对成功记录调用——"失败记录被路由进业务通道"这类歧义从模型上就不存在。
2. **成功的记录**：处理器实现了 `RoutingProcessor` → 用其返回的 key；没实现或返回 `null` → 进 `success` 通道。
3. **成功记录被指到 `"error"` key** → 无法落盘（错误通道只收失败行），引擎自动拨回 `success` 并告警一次。
4. **返回了未在 `build()` 声明的 key** → 不会动态建文件，而是兜底到 `error` 通道、计数并告警（只报首次，避免刷屏）。这样可把"拼写错误 / 多余空格"这类事故压缩成一条明确日志，而不是静默产出没人读的文件。

### 3.3 行格式：`RecordFormatter`（单方法）

```java
String format(T record, ItemResult<P> result);
```

**同一个 formatter 实例既服务成功通道也服务错误通道**——但路由保证一个通道只收到一种结果：

- 绑在普通通道（success / paid …）的 formatter，只会收到**成功**结果 → 读 `result.getPayload()`；
- 绑在错误通道的 formatter，只会收到**失败**结果 → 读 `result.getCode()` / `result.getMessage()`。

所以一个 lambda 既能当成功格式也能当错误格式，按"当前通道只看到哪种结果"来取字段即可。

---

## 4. SPI 契约（实现时必须遵守的约束）

### 4.1 `RecordReader<T>`（怎么读）

```java
public interface RecordReader<T> extends Closeable {
    boolean hasNext() throws IOException;   // 是否还有下一条
    T next() throws IOException;            // 下一条（仅在 hasNext() 为 true 后调用）
    void close();                            // 关流
}
```

- 用 `hasNext()/next()` 而非 `read() 返回 null`，结束语义明确、IO 异常原样穿透。
- 已有 `Iterator` / `Iterable` / `read()` 形态工具？用 `RecordReaders` 一行适配，原工具类不用改：
  - `RecordReaders.fromIterator(it)` / `fromIterable(iterable)`
  - `RecordReaders.fromReadFunction(exSupplier)`：`exSupplier.get() throws IOException`，返回 `null` 表示结束。
- **文件带表头/表尾**：不要在 Reader 里手写跳过逻辑，用 `RecordReaders` 的组合器叠加（都是装饰器，`close()` 委派给源）：
  - `skipping(reader, n)`：丢弃开头 n 条
  - `filtering(reader, predicate)`：只保留满足条件的记录（如去空行）
  - `mapping(reader, fn)`：转换形态，`fn` 允许抛 `IOException`
  - `headerAware(reader, isHeader)`：跳过**开头连续**匹配的表头行
  - `footerAware(reader, isFooter)`：丢弃**结尾连续**匹配的表尾行（内部预读一条来判定是否到结尾）

### 4.2 `BatchProcessor<T, P>`（一批怎么处理）

```java
List<ItemResult<P>> process(List<T> batch, long batchSeq) throws BatchFailedException;
```

- `batch`：本批记录（最后一批可能不足 batchSize）；`batchSeq`：批次序号（从 1 起，与文件位置顺序一致，可复现）。
- **返回值必须与入参 `batch` 一一对应、顺序一致**。框架据此把结果与记录对齐落盘。
- 成功 → `ItemResult.success(code, payload)`；业务失败 → `ItemResult.failed(code, message)`。
- **整批调用失败**（超时 / 5xx / 连接失败 / 解析失败）→ 抛 `BatchFailedException(code, msg[, cause])`，该批全部记录写入错误通道（错误码取自异常），然后继续处理后续批次。
- 返回列表大小与 batch 不符 → 框架防御性降级为整批失败（`BAD_RESPONSE`），避免记录不明丢失。

### 4.3 `RoutingProcessor<T, P>`（可选，按结果分流）

```java
public interface RoutingProcessor<T, P> extends BatchProcessor<T, P> {
    String channelOf(int indexInBatch, T record, ItemResult<P> result); // 只对成功记录调用
}
```

- 只有需要"不同结果写不同文件"时才实现它，属于增量能力；不实现时成功进 `success`、失败进 `error`，行为完全不变。
- `channelOf` **只对成功记录调用**，实现里**不必再写 `if (!result.isSuccess()) …`**。
- 返回 key（必须已声明）或 `null`（进 `success`）；返回未声明 key → 兜底 `error` 并计数。
- **实现必须线程安全**：多个消费者线程会并发调用，不同批次间无 happens-before 关系。
- 习惯做法：用 `result.getCode()`（稳定标识，如 `PAID`）映射通道，不要用 payload 里的业务文案（文案一改就全线失配）。`channelOf` 只做映射，不要在这里再查一次库（会与落盘内容不一致）。

### 4.4 `RecordFormatter<T, P>`（一行长什么样）

见 3.3。成功读 `payload`，失败读 `code/message`。

---

## 5. 装配与运行（最小骨架）

### 5.1 最简：成功 + 错误两个文件

```java
PipelineConfig<String, String> config = PipelineConfig.<String, String>builder()
        .reader(reader)                       // RecordReader 实现
        .processor(processor)                 // BatchProcessor 实现
        .batchSize(100).consumerThreads(4).bufferBatches(1)
        .charset(StandardCharsets.UTF_8)
        // 分片目录默认落系统临时区；想观察 parts/ 结构时再显式 .partsDir(dir)
        .channel("success", ChannelTargets.file(new File(outDir, "sample.success")),
                (r, res) -> r + "\t" + res.getCode() + "\t" + res.getPayload(), true)
        .errorChannel(ChannelTargets.file(new File(outDir, "sample.error")),
                (r, res) -> res.getCode() + "\t" + res.getMessage() + "\t" + r, true)
        .build();

BatchResult result = BatchPipelines.run(config);  // 一行式入口
System.out.println(result);                        // 读取数 / 成功失败 / 通道分布 / 耗时
```

### 5.2 多通道：按业务状态分流到多个文件 + 一个错误文件

```java
PipelineConfig<String, String> config = PipelineConfig.<String, String>builder()
        .reader(reader)
        .processor(new MyOrderProcessor())         // 同时 implements RoutingProcessor
        .channel("paid",      ChannelTargets.file(new File(dir, "order.paid")),      okFmt)
        .channel("shipped",   ChannelTargets.file(new File(dir, "order.shipped")),   okFmt)
        .channel("completed", ChannelTargets.file(new File(dir, "order.completed")), okFmt)
        .channel("cancelled", ChannelTargets.file(new File(dir, "order.cancelled")), okFmt)
        .errorChannel(ChannelTargets.file(new File(dir, "order.error")), errFmt)  // 兜底，必须
        .build();
```

处理器额外实现 `channelOf`：

```java
public String channelOf(int indexInBatch, String record, ItemResult<String> result) {
    if (!result.isSuccess()) return null;     // 实际上不会被调用（失败永远进 error）
    return statusToChannelKey(result.getCode()); // 例：PAID -> "paid"
}
```

**守恒关系**：每条记录恰好落一个通道，因此 `Σ各通道行数 = 输入条数`（批次级失败也整批计入错误通道，一条不丢）。下游对账时可用 `BatchResult.getChannelCounts()` 校验。

### 5.3 需要中途 abort / 读运行态时

改用实例形态：`new BatchPipeline<>(config).run()`，拿到引用后可调 `run()` / `abort(...)` / `getStats()`。

### 5.4 批量前按字段分组（内置分组，单组保证）

需求：读进来的记录不能原样进 `BatchProcessor`，而要**先按某字段分成多组**，让每次下游调用只含一个组（如按客户号/仓库分组，下游接口要求同组才放行）。

框架的"批"原本是**传输层概念**（按数量攒批，为并发与背压服务），分组是另一维度——内置分组把二者合一：启用后，框架每读一条记录就按分组键入桶，**某桶攒满 `batchSize` 即作为一个「单组批次」提交下游**。于是每次 `process()` 收到的 batch **只含同一个分组键**（单组保证），其余编排、背压、落盘逻辑不变。

启用方式（一行 SPI，其余代码零改动）：

```java
PipelineConfig.<MyRec, String>builder()
    .reader(reader).processor(processor)
    .groupBy(r -> r.getGroupKey())          // ← 内置分组：按字段取分组键（纯取值，不抛受检异常）
    .batchSize(50)                          // 作为“每桶 flush 阈值”
    .channel("success", ...).errorChannel(...)
    .build();
```

`groupBy` 接收一个 `GroupKeyExtractor<T, K>`（函数式接口，`K extract(T)`，不抛受检异常）。底层把"如何成批"抽成 `BatchingStrategy<T>` 策略：默认 `CountBatchingStrategy`（按数量切批），配置分组时用 `GroupBatchingStrategy`（按 key 分桶、攒满即切成 `batchSize` 大小的批次、EOF 兜底 flush）。不配置 `groupBy` 时框架按数量攒批，与配置分组时走同一套策略契约。

**关键事实（务必理解）**：

- **单组保证**：每次下游调用只含同一个分组键。整组超过 `batchSize` 会被切成多个"单组批次"，每个仍是单组；输入结束时各桶未满的尾批会兜底提交，**不丢数据**。
- **内存有界**：各桶合计 ≤ 组数 × (batchSize−1)，另加在途批次 (N+B) × batchSize，两项都与输入总量 N 无关——**不需要把全部记录读进内存**。这正是它优于"先全量读入再分组"的根本原因（后者 O(N) 内存，数据量大即 OOM）。
- **硬约束：分组键的不同值数量必须有界且较小（低基数）**，典型如租户/地区/渠道（几十到几百）。高基数（如按用户 ID）会撑爆内存，应改用**外部分文件预处理**（按 key 落不同文件后各自跑流水线），不要靠本能力。
- **未配置 `groupBy` 时**：框架按数量攒批（默认 `CountBatchingStrategy`），其余 SPI 契约不变。

**与「processor 内分组」的边界**：只想"同批里同组的记录一起发"，可在 `BatchProcessor` 内对框架按数量给的一批做 `groupBy` 再分别调下游——更省事，但分组只在当前这批内有效，组跨 batch 边界会被拆成两次调用。需要"**严格单组保证 + 内存有界**"时才用上面的内置 `groupBy`。

完整可运行示例见 `com.example.demo.DemoGroupMain`（输入 G1/G2/G3 交错，按 `key` 内置分组，输出 `output/group.success` 与 `group.error`；processor 会断言每批单组并打印组号）。

### 5.5 输出目标：不止本地文件

通道的**最终归宿**由 `ChannelTarget` 描述：

```java
public interface ChannelTarget {
    OutputStream open()   throws IOException;   // 打开暂存流，框架写入合并后的全部内容
    void         commit() throws IOException;   // 流关闭后调用，此时结果才对外可见
}
```

- 落本地文件：`.channel("success", ChannelTargets.file(new File(dir, "order.success")), fmt, true)`
- 落到 S3/OSS：实现 `ChannelTarget`（`open()` 返回本地临时文件流，`commit()` 上传后删除）——**框架不引入任何 SDK**。

**关键点：分片目录（`.partsDir(...)`）始终在本地**，它是并行阶段的暂存区，与输出目标解耦。好处是远程写入失败可重试 `commit()`，不必重跑整条流水线。

---

## 6. 参数（Builder 默认值）

| 参数 | 默认值 | 说明 |
|---|---|---|
| `batchSize` | 100 | 每批条数 |
| `consumerThreads` | 2 | 并行消费者线程数 N |
| `bufferBatches` | 1 | 缓冲位数 B；在途上限 M = N + B |
| `awaitTerminationSeconds` | 60 | 停机时等待在途批次的上限，超时强制 `shutdownNow()` |
| `charset` | UTF-8 | 读写编码 |
| `deletePartsOnSuccess` | true | 合并成功后清理分片目录 |
| `partsDir` | 系统临时目录 | 分片目录（本地暂存区，与输出目标解耦） |
| `groupExtractor` | null | 批量前按字段分组（opt-in，见 §5.4）；null = 按 `batchSize` 数量攒批 |

`PipelineConfig.maxInflightBatches()` = `consumerThreads + bufferBatches`。批处理通常远慢于"读 100 行文本"，`B=1` 基本就能消除消费者空等；`consumerThreads` 取决于后端并发承受能力。

---

## 7. 异常处理（给模型的事实）

| 场景 | 内核行为 |
|---|---|
| `read()/hasNext()` 抛异常 | 置停止标志 → 停止读取与提交 → 等在途批次跑完 → **不合并、不覆盖旧输出** → 抛 `PipelineExecutionException` |
| `process` 抛 `BatchFailedException` | 整批写入错误分片（错误码取自异常，如 `BATCH_*`）→ 后续继续 |
| `process` 抛其他异常 | 同上，错误码标 `UNKNOWN`，不拖垮整个文件 |
| 返回结果条数与批次不符 | 降级为整批失败（`BAD_RESPONSE`） |
| 写分片失败（磁盘满/权限） | 兜底失效 → 触发全局停止（不静默丢数据） |
| 致命错误（OOM 等） | 触发全局停止 |
| JVM 关闭信号 | 优雅停止：停止读取，等在途批次完成 |
| 等待在途批次超时 | `awaitTerminationSeconds` 后 `shutdownNow()` |

无论走哪条路径，`finally` 中都会归还信号量名额，背压不会泄漏。

---

## 8. 并发与背压（事实，模型无需改动）

- 生产者单线程读、攒批；消费者多线程并发 `process`。
- 背压 = 信号量控制在途批次数 M = N + B。生产者**先攒满一批再申请名额**（不是先占名额再读），因此无闲置空间时生产者停在 `acquire()` 上，不再读取、不再提交——严格符合"没空间就停产"。
- 内存上界 = (N + B) × batchSize 条记录，与文件大小无关。
- 并行阶段每个批次只写自己的分片（`parts/{key}/part-00000N.part`），零锁竞争；全部结束后单线程按批次序号升序合并，交给该通道的 `ChannelTarget` 提交——文件目标即"写同目录 `.tmp` → 原子 rename"，下游永远读不到写了一半的输出。
- 因批次序号连续、文件名零填充，**每个通道内部天然保持原始行序**。

---

## 9. AI 接入新需求的执行流程（落地清单）

当被要求"用这个框架做 X"时，按以下步骤，不要跳步：

1. **明确输入来源** → 选/写 `RecordReader`（或 `RecordReaders` 适配器）。把"一条原始记录"抽象成 `T`。
2. **明确"一批怎么处理"** → 写 `BatchProcessor`：真实调用接口/数据库，返回与 batch 一一对应的 `ItemResult` 列表；整批失败抛 `BatchFailedException`。
3. **明确出口** → 设计通道表：只需成功/错误两个？还是要按状态拆多个？错误通道必须声明。默认落本地文件用 `ChannelTargets.file(...)`；要落到 S3/OSS 等则实现 `ChannelTarget`（见 §5.5）。
4. **写 `RecordFormatter`**：普通通道读 `payload`，错误通道读 `code/message`；列顺序要稳定（下游按列解析）。
5. **若多通道** → 处理器实现 `RoutingProcessor.channelOf`（只处理成功记录，返回已声明 key，线程安全）。
6. **装配 `PipelineConfig`**：`reader` / `processor` / `channel(...)*` / `errorChannel(...)` / 参数；调 `build()`（缺 error 通道会立即报错，这是预期的安全网）。
7. **运行**：`BatchPipelines.run(config)`；用 `try/catch` 兜 `PipelineExecutionException`。
8. **验证守恒**：`Σ通道行数 == 输入条数`；批次失败时该批整批进 error；`mvn test` 保持全绿、`mvn compile` 零警告。

---

## 10. 编码与协作规约（生成代码时遵守）

- **仅 Java 8 语法**；内核零第三方运行时依赖（真实业务所需的 HTTP/DB 驱动由使用方自行引入）。
- **构建**：用 Maven（`mvn compile`），不要手写 javac 命令或维护 `out/` 目录。
- **不要修改 `com.example.batch` 的编排内核**（`BatchPipeline` 等生产者/消费者/合并逻辑），除非是修复 bug；若改了，先在 README/DESIGN 记录原因。像 `RecordReaders` 这类**纯工具/适配器类**是允许的增益——它们不改动引擎，只是给你一段可复用的胶水代码。**唯一被显式批准的、对成批层的受控扩展是"内置分组批处理"（见 §5.4，用 opt-in 的 `BatchingStrategy` 实现，不配置 `groupBy` 时行为完全不变）；除此外不要动 `produce()` 的攒批逻辑。**
- **业务代码放在新 package**（如 `com.yourcompany.xxx`）或**直接替换 `com.example.demo`** 的示例实现；示例只是演示，不是框架的一部分。
- **通道 key 用 ASCII**（`paid`/`shipped`），不要用中文状态名——key 会用作分片子目录名，中文目录名在 Windows 打包 / zip 解压 / CI 缓存时易乱码。业务文案放 formatter 里。
- **注释用中文**，说明"为什么"而非"是什么"；类/方法 javadoc 解释约束（尤其是 SPI 的一一对应、线程安全等）。
- **编译与测试门禁**：`mvn compile` 应保持 `-Xlint:all` 零警告（pom 已配置）；改动内核或 SPI 后必须 `mvn test` 全绿。
- **git 协作约定**：非经用户明确要求，不要 `push` 到 `origin/main`（当前 main 与 origin/main 存在分歧，若需推送须用 `git push --force-with-lease origin main`，务必先与用户确认）。提交信息用中文、聚焦单一意图。

---

## 11. 常见陷阱（避免模型踩坑）

- ❌ 忘记声明 `.errorChannel(...)` → `build()` 抛异常（这是设计上的强制安全网，不是 bug）。
- ❌ 在 `channelOf` 里判断 `!result.isSuccess()` → 多余，`channelOf` 只对成功记录调用。
- ❌ 用 `result.getPayload()` 当通道 key → payload 是给人看的文案，会变；用 `getCode()`。
- ❌ `process` 返回的列表大小/顺序与 batch 不一致 → 整批被降级为失败。
- ❌ 通道 key 拼写/空格与 `channel(...)` 声明不一致 → 静默兜底到 error（排查时看 `BatchResult.getChannelCounts()` 与告警日志）。
- ❌ 成功记录被 `channelOf` 指到 `"error"` → 自动拨回 `success` 并告警；错误通道只收失败行。
- ❌ 把 `RecordReader` / `BatchProcessor` / `channelOf` 写成有状态的、非线程安全的 → 多消费者并发会出错。
- ⚠️ 一个通道只用一个 formatter 渲染（成功通道只收成功行、错误通道只收失败行）。**同一通道内列数必须稳定**，否则下游按列解析会错位；Demo 的错误行是 `错误码 \t 错误信息 \t 原始记录`，不要为它单独加"级别"列。

---

## 12. 验证清单（交付前自检）

- [ ] `mvn clean test` 全绿（当前 27 个用例），`mvn compile` 零警告、零错误。
- [ ] 双输出：`success 行数 + error 行数 == 输入行数`。
- [ ] 多通道：`Σ各通道行数 == 输入条数`（看 `BatchResult.getChannelCounts()`）。
- [ ] 批次级失败时该批整批进 error（错误码为 `BATCH_*` 前缀）。
- [ ] 未注册通道 / 未知状态被兜底进 error 并告警（非静默丢失）。
- [ ] `parts/` 在 `deletePartsOnSuccess(true)` 下合并后被清理；需要观察时设 `partsDir(...)` + `deletePartsOnSuccess(false)`。
- [ ] 读取异常时整条流水线优雅停止，**且不覆盖**已存在的旧输出文件。
