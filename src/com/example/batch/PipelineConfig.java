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
 * 通道在 build() 时全部声明完毕，运行时只允许路由到已声明的 key（详见 {@link OutputChannel}）。
 *
 * <p>两种写法都支持，且老写法行为完全不变：
 * <pre>
 * // 老写法：两个固定输出
 * .successFile(f1).successFormatter(fmt).errorFile(f2).errorFormatter(fmt)
 *
 * // 新写法：按业务状态分流
 * .channel("paid",    new File(dir, "order.paid"),    fmt)
 * .channel("shipped", new File(dir, "order.shipped"), fmt)
 * .channel("error",   new File(dir, "order.error"),   fmt)
 * </pre>
 *
 * @param <T> 原始记录类型
 * @param <P> 附加内容类型
 */
public final class PipelineConfig<T, P> {

    private final RecordReader<T> reader;
    private final BatchProcessor<T, P> processor;
    private final LineFormatter<T, P> successFormatter;
    private final LineFormatter<T, P> errorFormatter;

    private final int batchSize;
    private final int consumerThreads;
    private final int bufferBatches;
    private final long awaitTerminationSeconds;
    private final Charset charset;
    private final boolean deletePartsOnSuccess;
    private final boolean createEmptyOutput;

    private final File partsDir;
    private final File successFile;
    private final File errorFile;

    /** 声明顺序即合并顺序，保证多次运行输出文件的生成次序稳定。 */
    private final Map<String, OutputChannel<T, P>> channels;
    private final String errorChannelKey;

    private PipelineConfig(Builder<T, P> b, Map<String, OutputChannel<T, P>> channels, String errorChannelKey) {
        this.reader = b.reader;
        this.processor = b.processor;
        this.successFormatter = b.successFormatter;
        this.errorFormatter = b.errorFormatter;
        this.batchSize = b.batchSize;
        this.consumerThreads = b.consumerThreads;
        this.bufferBatches = b.bufferBatches;
        this.awaitTerminationSeconds = b.awaitTerminationSeconds;
        this.charset = b.charset;
        this.deletePartsOnSuccess = b.deletePartsOnSuccess;
        this.createEmptyOutput = b.createEmptyOutput;
        this.partsDir = b.partsDir;
        this.successFile = b.successFile;
        this.errorFile = b.errorFile;
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

    public LineFormatter<T, P> getSuccessFormatter() {
        return successFormatter;
    }

    public LineFormatter<T, P> getErrorFormatter() {
        return errorFormatter;
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

    /** 全局默认值：仅用于把老 API（successFile/errorFile）转成通道时决定是否生成空文件。 */
    public boolean isCreateEmptyOutput() {
        return createEmptyOutput;
    }

    public File getPartsDir() {
        return partsDir;
    }

    /** 兼容老 API：{@code success} 通道的目标文件；未声明该通道时返回 null。 */
    public File getSuccessFile() {
        OutputChannel<T, P> ch = channels.get(OutputChannel.SUCCESS);
        return ch == null ? null : ch.getTarget();
    }

    /** 兼容老 API：错误通道的目标文件。 */
    public File getErrorFile() {
        OutputChannel<T, P> ch = channels.get(errorChannelKey);
        return ch == null ? null : ch.getTarget();
    }

    /** 全部通道，key → 通道定义。声明顺序稳定。 */
    public Map<String, OutputChannel<T, P>> getChannels() {
        return channels;
    }

    public OutputChannel<T, P> getChannel(String key) {
        return channels.get(key);
    }

    /** 错误通道的 key（默认 {@link OutputChannel#ERROR}）。 */
    public String getErrorChannelKey() {
        return errorChannelKey;
    }

    public static final class Builder<T, P> {

        private RecordReader<T> reader;
        private BatchProcessor<T, P> processor;
        private LineFormatter<T, P> successFormatter;
        private LineFormatter<T, P> errorFormatter;

        private int batchSize = 100;
        private int consumerThreads = 2;
        private int bufferBatches = 1;
        private long awaitTerminationSeconds = 60L;
        private Charset charset = Charset.forName("UTF-8");
        private boolean deletePartsOnSuccess = true;
        private boolean createEmptyOutput = true;

        private File partsDir;
        private File successFile;
        private File errorFile;

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

        public Builder<T, P> successFormatter(LineFormatter<T, P> v) {
            this.successFormatter = v;
            return this;
        }

        public Builder<T, P> errorFormatter(LineFormatter<T, P> v) {
            this.errorFormatter = v;
            return this;
        }

        /** 声明一个输出通道（推荐写法，可声明任意多个）。 */
        public Builder<T, P> channel(OutputChannel<T, P> channel) {
            if (channel == null) {
                throw new IllegalArgumentException("channel 不能为 null");
            }
            this.channels.put(channel.getKey(), channel);
            return this;
        }

        /** 声明一个输出通道的快捷写法，默认不生成空文件。 */
        public Builder<T, P> channel(String key, File target, LineFormatter<T, P> formatter) {
            return channel(OutputChannel.of(key, target, formatter));
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

        /** 没有任何成功/错误记录时，是否仍然生成空的输出文件（对老 API 的两个通道生效）。 */
        public Builder<T, P> createEmptyOutput(boolean v) {
            this.createEmptyOutput = v;
            return this;
        }

        public Builder<T, P> partsDir(File v) {
            this.partsDir = v;
            return this;
        }

        public Builder<T, P> successFile(File v) {
            this.successFile = v;
            return this;
        }

        public Builder<T, P> errorFile(File v) {
            this.errorFile = v;
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

            // 老 API → 通道：显式声明优先，不被覆盖
            if (!resolved.containsKey(OutputChannel.SUCCESS)
                    && successFile != null && successFormatter != null) {
                resolved.put(OutputChannel.SUCCESS,
                        OutputChannel.of(OutputChannel.SUCCESS, successFile, successFormatter)
                                .createEmpty(createEmptyOutput));
            }

            // 错误通道必须存在：记录级失败、批次级失败、未注册路由都写这里
            String errorKey = OutputChannel.ERROR;
            if (!resolved.containsKey(errorKey)) {
                if (errorFile == null || errorFormatter == null) {
                    throw new IllegalArgumentException(
                            "必须存在错误通道：请用 errorFile+errorFormatter，或显式声明 channel(\""
                                    + OutputChannel.ERROR + "\", ...)");
                }
                resolved.put(errorKey,
                        OutputChannel.error(errorFile, errorFormatter).createEmpty(createEmptyOutput));
            }

            if (resolved.isEmpty()) {
                throw new IllegalArgumentException("至少声明一个输出通道");
            }
            return new PipelineConfig<T, P>(this, resolved, errorKey);
        }
    }
}
