package com.example.batch;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 流水线配置（Builder 装配）。业务相关的能力全部由 SPI 注入，配置本身不含业务逻辑。
 *
 * <p>输出侧是一张<b>通道表</b>：每个通道 = 一个输出目标（{@link ChannelTarget}）+ 一套行格式
 * （{@link RecordFormatter}）。通道在 build() 时全部声明完毕，运行时只允许路由到已声明的 key：
 * <pre>
 * PipelineConfig.&lt;String, String&gt;builder()
 *     .reader(reader)
 *     .processor(processor)
 *     .channel("success", ChannelTargets.file(new File(dir, "order.success")), successFormatter)
 *     .channel("paid",    ChannelTargets.file(new File(dir, "order.paid")),    paidFormatter)
 *     .errorChannel(ChannelTargets.file(new File(dir, "order.error")), errFormatter)
 *     .build();
 * </pre>
 *
 * <p>三类约定（普通通道用 {@code channel(...)}，错误通道用专用的 {@code errorChannel(...)}）：
 * <ul>
 *   <li><b>success</b>：未实现 {@link RoutingProcessor} 时，所有成功记录的默认去向；
 *       实现了时它可作 {@code channelOf} 返回 null 的兜底。</li>
 *   <li><b>error</b>（key 固定为 {@code "error"}，由 {@link #errorChannel} 声明）：记录级失败、
 *       批次级失败、未注册路由全部落这里，是整条流水线的兜底安全网——缺失会在 {@link #build()} 时报错。</li>
 *   <li><b>其它 key</b>（paid / shipped …）：配合 {@link RoutingProcessor#channelOf} 按业务状态再分流。</li>
 * </ul>
 *
 * <p>所有通道共用同一个 {@link RecordFormatter} 类型：普通通道只看到成功记录，错误通道只看到失败记录，
 * 由路由保证。空文件策略是 per-channel 的：默认不生成，需要固定文件集的下游用带
 * {@code createEmpty} 参数的重载打开。
 *
 * @param <T> 原始记录类型
 * @param <P> 附加内容类型
 */
public final class PipelineConfig<T, P> {

    private final RecordReader<T> reader;
    private final BatchProcessor<T, P> processor;

    private final int batchSize;
    private final int consumerThreads;
    private final int bufferBatches;
    private final long awaitTerminationSeconds;
    private final Charset charset;
    private final boolean deletePartsOnSuccess;

    private final File partsDir;

    /** 声明顺序即合并顺序，保证多次运行输出文件的生成次序稳定。 */
    private final Map<String, OutputChannel<T, P>> channels;

    /** 分组键提取器（可选）。为 null 时按数量攒批（默认策略）；非 null 时启用内置分组。 */
    private final GroupKeyExtractor<T, ?> groupExtractor;

    private PipelineConfig(Builder<T, P> b, Map<String, OutputChannel<T, P>> channels) {
        this.reader = b.reader;
        this.processor = b.processor;
        this.batchSize = b.batchSize;
        this.consumerThreads = b.consumerThreads;
        this.bufferBatches = b.bufferBatches;
        this.awaitTerminationSeconds = b.awaitTerminationSeconds;
        this.charset = b.charset;
        this.deletePartsOnSuccess = b.deletePartsOnSuccess;
        this.partsDir = b.partsDir;
        this.channels = Collections.unmodifiableMap(channels);
        this.groupExtractor = b.groupExtractor;
    }

    public static <T, P> Builder<T, P> builder() {
        return new Builder<T, P>();
    }

    /** 在途批次上限 = 并行消费者数 + 缓冲位数。 */
    public int maxInflightBatches() {
        return consumerThreads + bufferBatches;
    }

    public RecordReader<T> getReader() {
        return reader;
    }

    public BatchProcessor<T, P> getProcessor() {
        return processor;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getConsumerThreads() {
        return consumerThreads;
    }

    public int getBufferBatches() {
        return bufferBatches;
    }

    public long getAwaitTerminationSeconds() {
        return awaitTerminationSeconds;
    }

    public Charset getCharset() {
        return charset;
    }

    public boolean isDeletePartsOnSuccess() {
        return deletePartsOnSuccess;
    }

    public File getPartsDir() {
        return partsDir;
    }

    /** 全部通道，key → 通道定义。声明顺序稳定。 */
    public Map<String, OutputChannel<T, P>> getChannels() {
        return channels;
    }

    public OutputChannel<T, P> getChannel(String key) {
        return channels.get(key);
    }

    /**
     * 分组键提取器；null 表示不分组（走默认按数量攒批策略）。
     * 非 null 时启用内置分组批处理：每条记录先按键入桶，某桶攒满 batchSize 即作为一个单组批次提交。
     */
    public GroupKeyExtractor<T, ?> getGroupExtractor() {
        return groupExtractor;
    }

    public static final class Builder<T, P> {

        private RecordReader<T> reader;
        private BatchProcessor<T, P> processor;

        private int batchSize = 100;
        private int consumerThreads = 2;
        private int bufferBatches = 1;
        private long awaitTerminationSeconds = 60L;
        private Charset charset = StandardCharsets.UTF_8;
        private boolean deletePartsOnSuccess = true;

        private File partsDir;

        /** 分组键提取器（可选）。null = 不分组，走默认按数量攒批。 */
        private GroupKeyExtractor<T, ?> groupExtractor;

        private final Map<String, OutputChannel<T, P>> channels =
                new LinkedHashMap<String, OutputChannel<T, P>>();

        public Builder<T, P> reader(RecordReader<T> v) {
            this.reader = v;
            return this;
        }

        public Builder<T, P> processor(BatchProcessor<T, P> v) {
            this.processor = v;
            return this;
        }

        /** 声明一个输出通道：成功或失败结果都交给同一个 {@link RecordFormatter} 渲染。 */
        public Builder<T, P> channel(String key, ChannelTarget target,
                                     RecordFormatter<T, P> formatter) {
            return channel(key, target, formatter, false);
        }

        /** 声明一个输出通道，并指定无数据时是否仍产出空结果。 */
        public Builder<T, P> channel(String key, ChannelTarget target,
                                     RecordFormatter<T, P> formatter, boolean createEmpty) {
            return add(OutputChannel.of(key, target, formatter, createEmpty));
        }

        /**
         * 声明错误通道（兜底安全网）：所有失败记录的归宿。
         *
         * <p>该通道 key 固定为 {@link OutputChannel#ERROR}（即 {@code "error"}），由本方法内部设定，
         * 调用方无需也不能改成别的名字——失败的路由目标在引擎里写死为这个 key。
         * 它是整条流水线的兜底：缺失会在 {@link #build()} 时报错，避免“首次失败才崩溃”。
         */
        public Builder<T, P> errorChannel(ChannelTarget target, RecordFormatter<T, P> formatter) {
            return errorChannel(target, formatter, false);
        }

        /** 声明错误通道，并指定无失败时是否仍产出空结果。 */
        public Builder<T, P> errorChannel(ChannelTarget target, RecordFormatter<T, P> formatter,
                                          boolean createEmpty) {
            return add(OutputChannel.of(OutputChannel.ERROR, target, formatter, createEmpty));
        }

        private Builder<T, P> add(OutputChannel<T, P> channel) {
            OutputChannel<T, P> prev = channels.put(channel.getKey(), channel);
            if (prev != null) {
                throw new IllegalArgumentException("通道 key 重复声明: " + channel.getKey());
            }
            return this;
        }

        public Builder<T, P> batchSize(int v) {
            this.batchSize = v;
            return this;
        }

        public Builder<T, P> consumerThreads(int v) {
            this.consumerThreads = v;
            return this;
        }

        public Builder<T, P> bufferBatches(int v) {
            this.bufferBatches = v;
            return this;
        }

        public Builder<T, P> awaitTerminationSeconds(long v) {
            this.awaitTerminationSeconds = v;
            return this;
        }

        public Builder<T, P> charset(Charset v) {
            this.charset = v;
            return this;
        }

        public Builder<T, P> deletePartsOnSuccess(boolean v) {
            this.deletePartsOnSuccess = v;
            return this;
        }

        public Builder<T, P> partsDir(File v) {
            this.partsDir = v;
            return this;
        }

        /**
         * 启用<b>内置分组批处理</b>：每条记录先按 {@code extractor} 取出分组键，再按键归桶；
         * 某个桶攒满 batchSize 即作为一个「单组批次」提交给 {@link com.example.batch.BatchProcessor}。
         *
         * <p>启用后，每次下游调用<b>只含同一个分组键</b>的记录（单组保证）；整组超过 batchSize 会被切成
         * 多个「单组批次」，每个仍是单组；输入结束时会把各桶未满的尾批兜底提交，不丢数据。
         *
         * <p><b>硬约束</b>：分组键的<b>不同值数量必须有界且较小（低基数）</b>——如租户 / 地区 / 渠道。
         * 高基数（如用户 ID）会撑爆内存，不适用本能力（需要的话应改用外部分文件预处理）。
         *
         * <p>不调用本方法（默认 null）时，框架使用 {@link CountBatchingStrategy} 按数量攒批：
         * 记录按到达顺序达到 batchSize 即提交一批。
         */
        public <K> Builder<T, P> groupBy(GroupKeyExtractor<T, K> extractor) {
            this.groupExtractor = extractor;
            return this;
        }

        public PipelineConfig<T, P> build() {
            if (reader == null) {
                throw new IllegalArgumentException("reader 必填");
            }
            if (processor == null) {
                throw new IllegalArgumentException("processor 必填");
            }
            if (batchSize <= 0) {
                throw new IllegalArgumentException("batchSize 必须大于 0");
            }
            if (consumerThreads <= 0) {
                throw new IllegalArgumentException("consumerThreads 必须大于 0");
            }
            if (bufferBatches < 0) {
                throw new IllegalArgumentException("bufferBatches 不能为负");
            }

            // 分片目录默认落到系统临时区；想观察 parts/ 结构时再显式指定。
            if (partsDir == null) {
                try {
                    partsDir = Files.createTempDirectory("batch-pipeline").toFile();
                } catch (IOException e) {
                    throw new IllegalStateException(
                            "无法创建默认分片目录，请用 partsDir(...) 显式指定", e);
                }
            }

            Map<String, OutputChannel<T, P>> resolved =
                    new LinkedHashMap<String, OutputChannel<T, P>>(channels);
            if (!resolved.containsKey(OutputChannel.ERROR)) {
                throw new IllegalArgumentException(
                        "必须声明错误通道（兜底安全网）：调用 errorChannel(target, formatter)。"
                                + "记录级失败、批次级失败与未注册路由都落这里，"
                                + "缺失会导致首次失败时整条流水线中止。");
            }
            return new PipelineConfig<T, P>(this, resolved);
        }
    }
}
