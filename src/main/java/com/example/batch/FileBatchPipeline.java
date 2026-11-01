package com.example.batch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 通用批处理流水线内核（与业务无关，全场景复用）。
 *
 * <p>结构：一个生产者线程负责读文件并攒批，N 个消费者线程并行处理批次，
 * 结束时把各批次的分片结果合并成统一输出文件。
 *
 * <p>背压：{@link Semaphore} 的许可数 = consumerThreads + bufferBatches。
 * 生产者在"攒满一批之后"才申请许可，因此在途未完成的批次数永远不会超过许可数——
 * 当所有消费者都在忙且缓冲占满时，生产者会停在 acquire 处停止读取。
 *
 * <p>线程模型：生产者跑在独立的 producer 线程；consumerThreads 为消费者线程数；
 * 调用线程负责等待结束、合并与收尾。
 */
public final class FileBatchPipeline<T, P> {

    private final PipelineConfig<T, P> config;
    private final PipelineStats stats = new PipelineStats();
    private final AtomicBoolean aborted = new AtomicBoolean(false);

    private volatile Throwable abortCause;
    private Semaphore semaphore;
    private ExecutorService pool;
    private PartWriter<T, P> partWriter;
    private ChannelRouter<T, P> router;
    private Thread shutdownHook;

    public FileBatchPipeline(PipelineConfig<T, P> config) {
        this.config = config;
    }

    public PipelineStats getStats() {
        return stats;
    }

    public boolean isAborted() {
        return aborted.get();
    }

    public Throwable getAbortCause() {
        return abortCause;
    }

    /** 触发停止：生产者停止读取、停止提交新批次；已在途的批次会继续跑完。 */
    public void abort(Throwable cause) {
        if (aborted.compareAndSet(false, true)) {
            abortCause = cause;
            PipelineLog.warn("流水线进入停止状态: %s",
                    cause == null ? "外部请求" : String.valueOf(cause.getMessage()));
        }
    }

    // ------------------------------------------------------------------ 主流程

    /**
     * 运行整个流水线，正常结束时返回只读统计快照。
     *
     * @throws PipelineExecutionException 读取失败、写分片失败、被中断或外部 abort 时抛出
     */
    public BatchResult run() throws PipelineExecutionException {
        stats.start();
        installShutdownHook();
        Throwable fatal = null;
        try {
            prepareDirectories();
            semaphore = new Semaphore(config.maxInflightBatches());
            pool = createPool();
            router = new ChannelRouter<T, P>(config);
            partWriter = new PartWriter<T, P>(config, router);
            PipelineLog.info("启动流水线: batchSize=%d consumers=%d buffer=%d maxInflight=%d channels=%s",
                    config.getBatchSize(), config.getConsumerThreads(), config.getBufferBatches(),
                    config.maxInflightBatches(), config.getChannels().keySet());

            Thread producer = new Thread(new Runnable() {
                public void run() {
                    produce();
                }
            }, "batch-producer");
            producer.start();
            producer.join();

            awaitPoolTermination();

            if (!aborted.get()) {
                mergeOutputs();
                if (config.isDeletePartsOnSuccess()) {
                    OutputMerger.cleanup(config.getPartsDir());
                }
            } else {
                PipelineLog.warn("流水线已提前停止，保留分片目录以便排查: %s",
                        config.getPartsDir().getAbsolutePath());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fatal = e;
            abort(e);
        } catch (Throwable t) {
            fatal = t;
            abort(t);
        } finally {
            releaseResources();
            reportUnregisteredChannels();
            stats.printSummary();
        }

        if (aborted.get() || fatal != null) {
            throw new PipelineExecutionException("流水线未正常完成: "
                    + (abortCause == null ? String.valueOf(fatal) : String.valueOf(abortCause.getMessage())),
                    abortCause != null ? abortCause : fatal);
        }
        return stats.snapshot(false, null);
    }

    // ------------------------------------------------------------------ 生产者

    private void produce() {
        final RecordReader<T> reader = config.getReader();
        final int batchSize = config.getBatchSize();
        // 攒批策略：不分组时按数量切批（默认），分组时按 key 入桶、满 batchSize 才整桶提交。
        // 两种策略都复用同一个 submitBatch（含背压与名额归还），其余编排逻辑不动。
        final BatchingStrategy<T> strat = (config.getGroupExtractor() == null)
                ? new CountBatchingStrategy<T>(batchSize)
                : new GroupBatchingStrategy<T>(batchSize, config.getGroupExtractor());
        long seq = 0;
        try {
            while (!aborted.get() && reader.hasNext()) {
                T record = reader.next();
                if (record == null) {
                    continue;
                }
                strat.add(record);
                stats.addRead(1);
                while (strat.hasReadyBatch()) {
                    submitBatch(++seq, strat.drainReadyBatch());
                }
            }
            if (!aborted.get()) {
                for (List<T> batch : strat.drainAll()) {
                    submitBatch(++seq, batch);   // 尾批：各桶未满部分兜底提交，不丢数据
                }
            }
            PipelineLog.info("生产者结束: 共读取 %d 条，提交 %d 批", stats.getReadCount(), seq);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            abort(e);
        } catch (IOException e) {
            // 读取异常按约定终止整个流程：不再新提交批次，等在途批次跑完
            PipelineLog.error("读取记录失败，触发全局停止: %s", String.valueOf(e.getMessage()));
            abort(e);
        } catch (Throwable t) {
            // 兜底：读取实现抛出的任何其他异常同样触发全局停止
            PipelineLog.error("读取记录失败，触发全局停止: %s", String.valueOf(t.getMessage()));
            abort(t);
        }
    }

    /** 先攒满一批、再申请名额：消费者慢时生产者手上最多多驻留 1 批，不会占着名额空等。 */
    private void submitBatch(long seq, List<T> batch) throws InterruptedException {
        acquirePermit();
        if (aborted.get()) {
            semaphore.release();
            throw new InterruptedException("aborted before submit");
        }
        try {
            pool.execute(new BatchTask<T, P>(this, config, stats, partWriter, semaphore, seq, batch));
            stats.addSubmitted();
        } catch (RejectedExecutionException e) {
            semaphore.release();
            abort(e);
            throw new InterruptedException("pool rejected: " + e.getMessage());
        }
    }

    private void acquirePermit() throws InterruptedException {
        boolean waited = false;
        while (!aborted.get()) {
            if (semaphore.tryAcquire(200, TimeUnit.MILLISECONDS)) {
                if (waited) {
                    PipelineLog.info("背压生效：名额已满，等待消费者归还后恢复提交");
                }
                return;
            }
            waited = true;
        }
        throw new InterruptedException("aborted while waiting for permit");
    }

    // ------------------------------------------------------------------ 收尾

    private void awaitPoolTermination() throws InterruptedException {
        pool.shutdown();
        if (!pool.awaitTermination(config.getAwaitTerminationSeconds(), TimeUnit.SECONDS)) {
            PipelineLog.warn("等待在途批次超时（%ds），强制关闭线程池", config.getAwaitTerminationSeconds());
            pool.shutdownNow();
            pool.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private void mergeOutputs() throws IOException {
        for (OutputChannel<T, P> channel : config.getChannels().values()) {
            File channelDir = channel.partsDirOf(config.getPartsDir());
            long lines = OutputMerger.merge(channelDir, channel.getTarget(),
                    config.getCharset(), channel.isCreateEmpty());
            PipelineLog.info("通道 %-12s → %-24s %d 行", channel.getKey(),
                    channel.getTarget().getName(), lines);
        }
    }

    /** 未注册通道是数据被错误分流的唯一信号，必须在收尾时显式告警，不能只留在日志中间。 */
    private void reportUnregisteredChannels() {
        if (router == null) {
            return;
        }
        if (router.unregisteredTotal() > 0L) {
            PipelineLog.warn("存在未注册的输出通道：共 %d 条记录被兜底到 \"%s\"，分布 %s",
                    router.unregisteredTotal(), config.getErrorChannelKey(),
                    router.unregisteredSnapshot());
        }
        if (router.successToErrorTotal() > 0L) {
            PipelineLog.warn("共 %d 条成功记录被指到错误通道，已拨回 \"%s\" 通道",
                    router.successToErrorTotal(), OutputChannel.SUCCESS);
        }
    }

    private void releaseResources() {
        if (pool != null && !pool.isShutdown()) {
            pool.shutdownNow();
        }
        try {
            config.getReader().close();
        } catch (Throwable ignored) {
            // 关闭失败不影响主流程
        }
        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
                // JVM 正在退出，忽略
            }
        }
    }

    // ------------------------------------------------------------------ 基础设施

    private ExecutorService createPool() {
        final int n = config.getConsumerThreads();
        ThreadFactory factory = new ThreadFactory() {
            private final AtomicInteger idx = new AtomicInteger(1);

            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "batch-consumer-" + idx.getAndIncrement());
                t.setDaemon(false);
                return t;
            }
        };
        // 队列不设上限：在途批次数由信号量严格控制，队列再大也不会多提交
        return new ThreadPoolExecutor(n, n, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), factory);
    }

    private void prepareDirectories() throws IOException {
        File partsDir = config.getPartsDir();
        if (partsDir.exists()) {
            // 多通道下 partsDir 里是各通道的子目录，必须递归清理，否则残留分片会被合并进本次输出
            OutputMerger.deleteChildren(partsDir);
        }
        mkdirs(partsDir);
        for (OutputChannel<T, P> channel : config.getChannels().values()) {
            mkdirs(channel.getTarget().getParentFile());
        }
    }

    private void mkdirs(File dir) throws IOException {
        if (dir == null) {
            return;
        }
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("无法创建目录: " + dir);
        }
    }

    private void installShutdownHook() {
        shutdownHook = new Thread(new Runnable() {
            public void run() {
                PipelineLog.warn("收到 JVM 关闭信号，请求优雅停止");
                abort(new IllegalStateException("JVM shutdown"));
            }
        }, "batch-shutdown-hook");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }
}
