package com.example.batch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 流水线统计指标（线程安全）。
 *
 * <p>内部计数、对外只读：写入方法仅供内核同包调用，使用方通过
 * {@link FileBatchPipeline#getStats()} 或 {@link BatchResult} 读取。
 */
public final class PipelineStats {

    private final AtomicLong readCount = new AtomicLong();
    private final AtomicLong batchSubmitted = new AtomicLong();
    private final AtomicLong batchOk = new AtomicLong();
    private final AtomicLong batchFailed = new AtomicLong();
    private final AtomicLong recordSuccess = new AtomicLong();
    private final AtomicLong recordFailed = new AtomicLong();
    private final AtomicLong recordBatchFailed = new AtomicLong();
    private final ConcurrentHashMap<String, AtomicLong> errorCodeCounts =
            new ConcurrentHashMap<String, AtomicLong>();
    /** 各输出通道累计写入行数：用于回答"某个通道这次有没有数据"。 */
    private final ConcurrentHashMap<String, AtomicLong> channelCounts =
            new ConcurrentHashMap<String, AtomicLong>();

    private volatile long startTimeNanos;
    private volatile long endTimeNanos;

    void start() {
        startTimeNanos = System.nanoTime();
    }

    void stop() {
        endTimeNanos = System.nanoTime();
    }

    void addRead(int n) {
        readCount.addAndGet(n);
    }

    void addSubmitted() {
        batchSubmitted.incrementAndGet();
    }

    void addBatchOk() {
        batchOk.incrementAndGet();
    }

    void addBatchFailed() {
        batchFailed.incrementAndGet();
    }

    void addRecordSuccess() {
        recordSuccess.incrementAndGet();
    }

    void addRecordFailed() {
        recordFailed.incrementAndGet();
    }

    void addRecordBatchFailed() {
        recordBatchFailed.incrementAndGet();
    }

    /** 累加某个输出通道本次写入的行数。 */
    void addChannel(String key, int n) {
        if (key == null || n <= 0) {
            return;
        }
        AtomicLong counter = channelCounts.get(key);
        if (counter == null) {
            counter = new AtomicLong();
            AtomicLong prev = channelCounts.putIfAbsent(key, counter);
            if (prev != null) {
                counter = prev;
            }
        }
        counter.addAndGet(n);
    }

    /** 记录错误码出现次数（成功码也记录，便于完整核对）。 */
    void addCode(String code) {
        if (code == null) {
            code = "-";
        }
        AtomicLong counter = errorCodeCounts.get(code);
        if (counter == null) {
            counter = new AtomicLong();
            AtomicLong prev = errorCodeCounts.putIfAbsent(code, counter);
            if (prev != null) {
                counter = prev;
            }
        }
        counter.incrementAndGet();
    }

    public long getReadCount() {
        return readCount.get();
    }

    /** 通道写入行数快照（key → 行数）。 */
    public Map<String, Long> getChannelCounts() {
        return snapshot(channelCounts);
    }

    public long elapsedMillis() {
        if (startTimeNanos == 0) {
            return 0;
        }
        long end = endTimeNanos == 0 ? System.nanoTime() : endTimeNanos;
        return (end - startTimeNanos) / 1000000L;
    }

    /** 生成本次运行的只读结果快照（供 {@code run()} 返回）。 */
    BatchResult snapshot(boolean aborted, Throwable abortCause) {
        return new BatchResult(
                readCount.get(), batchSubmitted.get(), batchOk.get(), batchFailed.get(),
                recordSuccess.get(), recordFailed.get(), recordBatchFailed.get(),
                snapshot(channelCounts), snapshot(errorCodeCounts),
                elapsedMillis(), aborted, abortCause);
    }

    private static Map<String, Long> snapshot(ConcurrentHashMap<String, AtomicLong> map) {
        Map<String, Long> snapshot = new java.util.LinkedHashMap<String, Long>();
        for (Map.Entry<String, AtomicLong> e : map.entrySet()) {
            snapshot.put(e.getKey(), e.getValue().get());
        }
        return snapshot;
    }

    /** 打印汇总：行数、批次、成功/失败、状态码分布、耗时。 */
    void printSummary() {
        stop();
        long failedRecord = recordFailed.get() + recordBatchFailed.get();
        long elapsed = elapsedMillis();
        PipelineLog.info("========== 批处理汇总 ==========");
        PipelineLog.info("读取记录数        : %d", readCount.get());
        PipelineLog.info("提交批次数        : %d", batchSubmitted.get());
        PipelineLog.info("成功批次数        : %d", batchOk.get());
        PipelineLog.info("批次级失败批次数  : %d", batchFailed.get());
        PipelineLog.info("成功记录数        : %d", recordSuccess.get());
        PipelineLog.info("记录级失败数      : %d", recordFailed.get());
        PipelineLog.info("批次失败牵连记录数: %d", recordBatchFailed.get());
        PipelineLog.info("失败记录合计      : %d", failedRecord);
        PipelineLog.info("总耗时            : %d ms", elapsed);
        if (batchSubmitted.get() > 0) {
            PipelineLog.info("平均每批耗时      : %d ms", elapsed / batchSubmitted.get());
        }
        PipelineLog.info("----- 通道分布（写入行数） -----");
        printSorted(channelCounts);
        PipelineLog.info("----- 状态码分布 -----");
        printSorted(errorCodeCounts);
        PipelineLog.info("================================");
    }

    private static void printSorted(ConcurrentHashMap<String, AtomicLong> map) {
        List<Map.Entry<String, AtomicLong>> entries =
                new ArrayList<Map.Entry<String, AtomicLong>>(map.entrySet());
        Collections.sort(entries, new java.util.Comparator<Map.Entry<String, AtomicLong>>() {
            public int compare(Map.Entry<String, AtomicLong> a, Map.Entry<String, AtomicLong> b) {
                return Long.compare(b.getValue().get(), a.getValue().get());
            }
        });
        for (Map.Entry<String, AtomicLong> e : entries) {
            PipelineLog.info("%-16s %d", e.getKey(), e.getValue().get());
        }
    }
}
