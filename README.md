# 通用批处理流水线模板（Java）

> 完整设计说明（架构图、流程图、时序图、接口契约、异常矩阵）见 **[DESIGN.md](./DESIGN.md)**。

面向**"有终点、有大批量数据"**的批处理场景：单线程读文件 → 每 100 条打包成一批 → 多线程并发调用批量 HTTP 接口 → 成功记录合并成主输出文件、失败记录合并成错误文件。

零第三方依赖，**纯 JDK 即可编译运行**（代码按 Java 8 兼容语法编写）。

---

## 一、快速开始

```bash
cd java-batch-pipeline
chmod +x run.sh
./run.sh
```

脚本会编译源码，并以两种方式各跑一次 Demo：

| 运行方式 | 说明 |
|---|---|
| `java -cp out/classes com.example.demo.DemoMain` | 专用 `RecordReader` 实现读取 |
| `java -cp out/classes com.example.demo.DemoMain demo-run adapter` | 用适配器把 `read()` 形态的读取工具接进来 |

产物位于 `demo-run/`：

```
demo-run/
├── input/sample.txt        示例输入（自动生成 1050 行，故意不是 100 的整数倍，用于验证尾批）
├── parts/                  分片目录（合并成功后自动清理）
└── output/
    ├── sample.success      主输出：仅成功记录
    └── sample.error        错误文件：失败记录（含级别列）
```

---

## 二、目录结构

```
src/com/example/batch/            ← 与业务无关的通用内核（不要改）
├── RecordReader.java           读取接口（SPI）
├── RecordReaders.java          Iterator / read() 形态的适配器
├── BatchProcessor.java         批处理接口（SPI，通常是调 HTTP）
├── RoutingProcessor.java       按结果分流到不同通道（SPI 的可选扩展）
├── BatchFailedException.java   批次级失败异常（携带错误码）
├── ItemResult.java / OutcomeStatus.java   单条结果模型
├── LineFormatter.java          输出行格式化接口（SPI）
├── OutputChannel.java          输出通道 = 一个目标文件 + 一套行格式
├── ChannelRouter.java          通道路由 + 未注册通道兜底
├── PipelineConfig.java         配置 + Builder（通道表 + 参数校验）
├── PipelineStats.java          统计（行数/批次/通道/状态码/耗时）
├── PartWriter.java             分片写入（按通道路由到各自目录）
├── OutputMerger.java           分片合并 + 原子 rename
├── BatchTask.java              单批任务（异常降级 + 名额归还）
├── FileBatchPipeline.java      内核编排（生产者/背压/合并/停机）
└── PipelineLog.java            极简日志门面（可换成 SLF4J）

src/com/example/demo/            ← 业务方要写的部分（替换成你的真实实现）
├── DemoLineReader.java         RecordReader 的一种实现（文本文件按行读）
├── DemoHttpProcessor.java      BatchProcessor 的一种实现（模拟批量 HTTP）
├── DemoLineFormatter.java      LineFormatter 的实现（平铺格式）
├── DemoMain.java               装配 + 运行（双输出：成功 / 错误）
├── DemoOrderStatus.java        订单状态枚举（其中一个状态故意不注册通道）
├── DemoOrderProcessor.java     RoutingProcessor 实现：按查询到的状态分流
└── DemoOrderMain.java          装配 + 运行（多通道：4 个业务文件 + 1 个错误文件）
```

---

## 三、核心机制

### 1. 背压：信号量控制在途批次数

```
在途上限 M = consumerThreads(N) + bufferBatches(B)
```

生产者的顺序是**先攒满 100 条，再申请名额**（而不是先占名额再读），因此：

- 当 N 个消费者都在忙、且 B 个缓冲位也被占满时，生产者停在 `acquire()` 上，不再读取、不再提交——**严格符合"没有闲置空间就停止生产"**；
- 缓冲位 B 的作用：消费者处理当前批次时，生产者可以提前攒好 B 批备着，消除等待抖动；
- 实际等待时日志会打印 `背压生效：名额已满，等待消费者归还后恢复提交`。

> 内存上界 = (N + B) × batchSize 条记录，与文件大小无关。

### 2. 分流：成功 / 失败进不同文件

| 情况 | 归属 | 级别列 |
|---|---|---|
| 接口正常返回且记录成功 | 主输出文件 | — |
| 接口正常返回但该条业务失败 | 错误文件 | `RECORD` |
| 整批调用失败（超时/5xx/连接失败/解析失败） | 错误文件（整批） | `BATCH` |

批次级失败时整批记录都会被写入错误文件，**一条都不会丢**。

### 3. 合并：分片 + 后置合并 + 原子落地

并行阶段每个批次只写自己的分片文件（`parts/paid/part-000007.part`），零锁竞争；全部结束后单线程按批次序号升序流式合并，先写 `*.tmp` 再 `rename`，下游**永远读不到写了一半的文件**。

因为批次序号是连续分配的、且文件名零填充，所以**每个通道内部天然保持原始行序**（虽然本需求不要求，但排查时很省事）。

### 4. 多通道：按业务状态分流到不同文件

默认两个输出（成功 / 错误）足够时不用管这一节。当"不同结果要写不同文件"时——比如批量查订单状态，按 **已付款 / 已发货 / 已完成 / 已取消** 各出一个文件——用通道表：

```java
PipelineConfig.<String, String>builder()
    .reader(...).processor(new MyOrderProcessor())
    .partsDir(new File(dir, "parts"))
    .channel("paid",      new File(dir, "order.paid"),      fmt)
    .channel("shipped",   new File(dir, "order.shipped"),   fmt)
    .channel("completed", new File(dir, "order.completed"), fmt)
    .channel("cancelled", new File(dir, "order.cancelled"), fmt)
    .channel(OutputChannel.<String, String>error(new File(dir, "order.error"), fmt))
    .build();
```

处理器额外实现 `RoutingProcessor`，为每条记录给出通道 key：

```java
public String channelOf(int indexInBatch, String record, ItemResult<String> result) {
    if (!result.isSuccess()) return null;          // null = 走默认（失败 → error）
    return statusChannel(result.getCode());         // 例：PAID → "paid"
}
```

**三条硬规则**：

| 规则 | 原因 |
|---|---|
| 通道必须在 `build()` 时全部声明，运行期只允许路由到已声明的 key | 避免拼写错误/多余空格静默产出没人读的文件；未注册的 key 一律兜底到 `error` 并计数告警 |
| 兜底记录强制走错误格式（`UNROUTED:xxx`） | 否则错误文件里会混入列数不同的行，下游按列解析直接错位 |
| 通道 key 用 ASCII（`paid`），不要用中文状态名 | key 会作为分片子目录名，中文目录名跨 Windows 打包 / zip 解压容易乱码 |

**守恒关系**：每条记录恰好落一个通道，因此 `Σ各通道行数 = 输入条数`（批次级失败也整批计入错误通道，一条不丢）。

**分片布局**：

```
parts/
├── paid/part-000001.part
├── shipped/part-000001.part
└── error/part-000002.part      ← 未注册通道与查询失败都落这里
```

按通道分子目录，而不是 `part-000001.paid.part`：合并时扫一个目录就够，通道 key 也不必参与文件名转义。

**某个通道这次没有记录**：不会有分片文件，合并阶段自然就知道——不需要额外统计。若下游必须拿到固定文件，用 `channel.createEmpty(true)` 单独打开（`error` 通道建议打开，便于告警侧统一处理）。

---

## 四、接入你的真实业务（只需三步）

**第 1 步：实现 `RecordReader`**（或直接适配已有工具）

```java
public class MyReader implements RecordReader<MyRecord> {
    public boolean hasNext() throws Exception { /* … */ }
    public MyRecord next()  throws Exception { /* … */ }
    public void close() { /* 关流 */ }
}

// 手上已有 read() 形态的工具类？一行适配器搞定，原工具类不用改：
RecordReader<String> reader = RecordReaders.fromReadFunction(() -> myLegacyTool.read());

// 手上已是 Iterator？
RecordReader<String> reader = RecordReaders.fromIterator(myIterator);
```

**第 2 步：实现 `BatchProcessor`**（调真实的批量 HTTP 接口）

```java
public class MyProcessor implements BatchProcessor<MyRecord, MyPayload> {
    public List<ItemResult<MyPayload>> process(List<MyRecord> batch, long batchSeq)
            throws BatchFailedException {
        Response resp = httpClient.batchCall(batch);          // 一次请求带 100 条
        List<ItemResult<MyPayload>> out = new ArrayList<>(batch.size());
        for (int i = 0; i < batch.size(); i++) {
            Item item = resp.get(i);
            if (item.isOk()) out.add(ItemResult.success(item.getCode(), item.getPayload()));
            else            out.add(ItemResult.failed(item.getCode(), item.getMsg()));
        }
        return out;                                            // 必须与 batch 一一对应
    }
}
```

> 成功/失败的判定由你的 Processor 决定，**内核不解读业务状态码的含义**，因此换任何接口都能复用同一套内核。

**第 3 步：实现 `LineFormatter`**（每种文件类型一套格式）

```java
public class MyFormatter implements LineFormatter<MyRecord, MyPayload> {
    public String formatSuccess(MyRecord r, ItemResult<MyPayload> res)      { return r.raw()+"\t"+res.getCode()+"\t"+res.getPayload(); }
    public String formatRecordFailed(MyRecord r, ItemResult<MyPayload> res) { return r.raw()+"\tRECORD\t"+res.getCode()+"\t"+res.getMessage(); }
    public String formatBatchFailed(MyRecord r, String code, String msg)    { return r.raw()+"\tBATCH\t"+code+"\t"+msg; }
}
```

然后装配运行：

```java
new FileBatchPipeline<>(PipelineConfig.<MyRecord, MyPayload>builder()
        .reader(reader).processor(processor)
        .successFormatter(formatter).errorFormatter(formatter)
        .batchSize(100).consumerThreads(4).bufferBatches(1)
        .partsDir(partsDir).successFile(successFile).errorFile(errorFile)
        .build()).run();
```

---

## 五、参数表

| 参数 | 默认值 | 说明 |
|---|---|---|
| `batchSize` | 100 | 每批条数 |
| `consumerThreads` | 2 | 消费者线程数 N（真正并行处理的线程数） |
| `bufferBatches` | 1 | 缓冲位数 B，在途上限 M = N + B |
| `awaitTerminationSeconds` | 60 | 停止时等待在途批次的上限，超限强制关闭线程池 |
| `charset` | UTF-8 | 读写编码 |
| `deletePartsOnSuccess` | true | 合并成功后清理分片目录 |
| `createEmptyOutput` | true | 无成功/错误记录时是否仍生成空文件 |

**调参建议**：批处理（写库/调接口）通常远慢于"读 100 行文本"，所以 `B = 1` 基本就能消除消费者空等；`B` 再往上加只是白占内存。`consumerThreads` 则取决于接口服务端的并发承受能力。

---

## 六、异常处理矩阵

| 场景 | 内核行为 |
|---|---|
| `read()` / `hasNext()` 抛异常 | 置停止标志 → 停止读取与提交 → 等在途批次跑完 → **不合并、不覆盖旧输出文件** → 抛 `PipelineExecutionException` |
| `BatchProcessor` 抛 `BatchFailedException` | 整批写入错误分片（级别 `BATCH`，错误码取自异常）→ 后续批次继续 |
| `BatchProcessor` 抛其他异常 | 同上，错误码标 `UNKNOWN`，不拖垮整个文件 |
| 返回结果条数与批次不符 | 防御降级为整批失败（`BAD_RESPONSE`），避免记录不明丢失 |
| 写分片失败（磁盘满/权限） | 兜底失效 → 触发全局停止（不静默丢数据） |
| 致命错误（OOM 等） | 触发全局停止 |
| JVM 关闭信号（Ctrl+C） | 优雅停止：停止读取，等在途批次完成 |
| 等待在途批次超时 | `awaitTerminationSeconds` 后 `shutdownNow()` 强制关闭 |

无论走哪条路径，`finally` 中都会归还信号量名额，背压不会泄漏。

---

## 七、本模板已验证的行为

| # | 验证项 | 结果 |
|---|---|---|
| 1 | 1050 行 → 11 批（含不足 100 条的尾批 50 条） | PASS |
| 2 | 数据统计守恒：成功 + 记录级失败 + 批次牵连 == 读取总数 | PASS |
| 3 | 输出文件行数守恒：主输出行数 + 错误文件行数 == 输入行数 | PASS |
| 4 | 批次级失败时整批 100 条落错误文件且标记 `BATCH` | PASS |
| 5 | 记录级失败标记 `RECORD`，两者可区分 | PASS |
| 6 | 消费慢时触发背压（生产者停止提交） | PASS |
| 7 | 合并后输出保持输入行序 | PASS |
| 8 | 读取异常时全局停止，且**不覆盖**已有输出文件 | PASS |
| 9 | 读取异常时已提交批次仍完整处理完毕 | PASS |
| 10 | 原子 rename：下游不会读到半截文件 | PASS |

---

## 八、已知边界与可选扩展

- **输出行序**：当前按批次序号升序合并（白送的行序保证），如需严格保证请保持该默认。
- **批次结果汇总**：如需把每批处理明细（批次号、耗时、成功/失败条数）额外导出，可在 `BatchTask` 中增加一个 `ResultCollector` 回调。
- **进程级断点续跑**：停止时保留 `parts/` 目录，据此可以实现"从失败批次之后继续"，需要时再扩展——当前版本未内置。
- **日志对接**：`PipelineLog` 是极简门面，把方法体换成 SLF4J 调用即可，无需改动业务代码。
- **HTTP 客户端替换**：Demo 中是模拟实现，换成 OkHttp / RestTemplate / WebClient 时请注意设置合理的超时时间。
