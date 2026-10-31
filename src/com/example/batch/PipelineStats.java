package com.example.batch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** 流水线统计指标（线程安全）。 */
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

    private volatile long startTimeNanos;
    private volatile long endTimeNanos;

    public void start() {
        startTimeNanos = System.nanoTime();
    }

    public void stop() {
        endTimeNanos = System.nanoTime();
    }

    public void addRead(int n) {
        readCount.addAndGet(n);
    }

    public void addSubmitted() {
        batchSubmitted.incrementAndGet();
    }

    public void addBatchOk() {
        batchOk.incrementAndGet();
    }

    public void addBatchFailed() {
        batchFailed.incrementAndGet();
    }

    public void addRecordSuccess() {
        recordSuccess.incrementAndGet();
    }

    public void addRecordFailed() {
        recordFailed.incrementAndGet();
    }

    public void addRecordBatchFailed() {
        recordBatchFailed.incrementAndGet();
    }

    /** 记录错误码出现次数（成功码也记录，便于完整核对）。 */
    public void addCode(String code) {
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

    public long getBatchSubmitted() {
        return batchSubmitted.get();
    }

    public long getBatchFailed() {
        return batchFailed.get();
    }

    public long getRecordSuccess() {
        return recordSuccess.get();
    }

    public long getRecordFailed() {
        return recordFailed.get();
    }

    public long getRecordBatchFailed() {
        return recordBatchFailed.get();
    }

    public long elapsedMillis() {
        if (startTimeNanos == 0) {
            return 0;
        }
        long end = endTimeNanos == 0 ? System.nanoTime() : endTimeNanos;
        return (end - startTimeNanos) / 1000000L;
    }

    /** 打印汇总：行数、批次、成功/失败、状态码分布、耗时。 */
    public void printSummary() {
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
        PipelineLog.info("----- 状态码分布 -----");
        List<Map.Entry<String, AtomicLong>> entries =
                new ArrayList<Map.Entry<String, AtomicLong>>(errorCodeCounts.entrySet());
        Collections.sort(entries, new java.util.Comparator<Map.Entry<String, AtomicLong>>() {
            public int compare(Map.Entry<String, AtomicLong> a, Map.Entry<String, AtomicLong> b) {
                return Long.compare(b.getValue().get(), a.getValue().get());
            }
        });
        for (Map.Entry<String, AtomicLong> e : entries) {
            PipelineLog.info("%-16s %d", e.getKey(), e.getValue().get());
        }
        PipelineLog.info("================================");
    }
}
