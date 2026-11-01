package com.example.batch;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 流水线配置（Builder 装配）。业务相关的能力全部由 SPI 注入，配置本身不含业务逻辑。
 *
 * <p>输出侧是一张**通道表**：每个通道 = 一个目标文件 + 一套行格式。
 * 通道在 build() 时全部声明完毕，运行时只允许路由到已声明的 key（详见 {@link OutputChannel}）：
 * <pre>
 * PipelineConfig.&lt;String, String&gt;builder()
 *     .reader(reader).processor(processor)
 *     .channel("paid",    new File(dir, "order.paid"),    fmt)
 *     .channel("shipped", new File(dir, "order.shipped"), fmt)
 *     .channel(OutputChannel.&lt;String, String&gt;error(new File(dir, "order.error"), fmt))
 *     .build();
 * </pre>
 *
 * <p>两条硬性规则：
 * <ul>
 *   <li><b>错误通道必须存在</b>：记录级失败、批次级失败、未注册路由都写这里，
 *       用 {@link OutputChannel#error} 声明；</li>
 *   <li><b>空文件策略是 per-channel 的</b>：默认不生成，需要固定文件集的下游
 *       对单个通道调 {@code createEmpty(true)}。</li>
 * </ul>
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
    private final String errorChannelKey;

    private PipelineConfig(Builder<T, P> b, Map<String, OutputChannel<T, P>> channels, String errorChannelKey) {
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
        this.errorChannelKey = errorChannelKey;
    }

    public static <T, P> Builder<T, P> builder() {
        return new Builder<T, P>();
    }

    /** 在途批次上限 = 并行消费者数 + 缓冲位数。小于 consumerThreads 时自动抬升。 */
    public int maxInflightBatches() {
        return Math.max(consumerThreads, consumerThreads + bufferBatches);
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

    /** 错误通道的 key（固定为 {@link OutputChannel#ERROR}）。 */
    public String getErrorChannelKey() {
        return errorChannelKey;
    }

    public static final class Builder<T, P> {

        private RecordReader<T> reader;
        private BatchProcessor<T, P> processor;

        private int batchSize = 100;
        private int consumerThreads = 2;
        private int bufferBatches = 1;
        private long awaitTerminationSeconds = 60L;
        private Charset charset = Charset.forName("UTF-8");
        private boolean deletePartsOnSuccess = true;

        private File partsDir;

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

        /** 声明一个输出通道（可声明任意多个，声明顺序即合并顺序）。 */
        public Builder<T, P> channel(OutputChannel<T, P> channel) {
            if (channel == null) {
                throw new IllegalArgumentException("channel 不能为 null");
            }
            this.channels.put(channel.getKey(), channel);
            return this;
        }

        /** 声明一个普通输出通道的快捷写法，默认不生成空文件。 */
        public Builder<T, P> channel(String key, File target, LineFormatter<T, P> formatter) {
            return channel(OutputChannel.of(key, target, formatter));
        }

        /** 声明错误通道的快捷写法：承载记录级失败、批次级失败与未注册路由。 */
        public Builder<T, P> errorChannel(File target, LineFormatter<T, P> formatter) {
            return channel(OutputChannel.error(target, formatter));
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
            if (partsDir == null) {
                throw new IllegalArgumentException("partsDir 必填");
            }

            Map<String, OutputChannel<T, P>> resolved =
                    new LinkedHashMap<String, OutputChannel<T, P>>(channels);

            // 错误通道必须存在且必须声明为错误角色：记录级失败、批次级失败、未注册路由都写这里
            String errorKey = OutputChannel.ERROR;
            OutputChannel<T, P> errorChannel = resolved.get(errorKey);
            if (errorChannel == null) {
                throw new IllegalArgumentException(
                        "必须声明错误通道：请用 errorChannel(file, formatter)，"
                                + "或 channel(OutputChannel.error(file, formatter))");
            }
            if (!errorChannel.isErrorChannel()) {
                throw new IllegalArgumentException(
                        "通道 \"" + errorKey + "\" 已声明，但不是错误通道："
                                + "请改用 errorChannel(file, formatter) 声明，"
                                + "否则兜底与批次级失败会写进一个普通通道");
            }

            return new PipelineConfig<T, P>(this, resolved, errorKey);
        }
    }
}
