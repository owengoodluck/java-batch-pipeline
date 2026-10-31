package com.example.batch;

import java.io.File;
import java.nio.charset.Charset;

/**
 * 流水线配置（Builder 装配）。业务相关的能力全部由 SPI 注入，配置本身不含业务逻辑。
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

    private PipelineConfig(Builder<T, P> b) {
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

    public boolean isCreateEmptyOutput() {
        return createEmptyOutput;
    }

    public File getPartsDir() {
        return partsDir;
    }

    public File getSuccessFile() {
        return successFile;
    }

    public File getErrorFile() {
        return errorFile;
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

        /** 没有任何成功/错误记录时，是否仍然生成空的输出文件（便于下游统一处理）。 */
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
            if (successFormatter == null || errorFormatter == null) {
                throw new IllegalArgumentException("successFormatter / errorFormatter 必填");
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
            if (successFile == null || errorFile == null || partsDir == null) {
                throw new IllegalArgumentException("successFile / errorFile / partsDir 必填");
            }
            return new PipelineConfig<T, P>(this);
        }
    }
}
