package com.example.batch;

import java.util.Map;

/**
 * 一次流水线运行的只读结果快照（{@link FileBatchPipeline#run} 的返回值）。
 *
 * <p>守恒式：{@code readCount == recordSuccess + recordFailed + recordBatchFailed}。
 * 各通道写入行数合计在「严格分流」场景下也应等于 {@code readCount}。
 */
public final class BatchResult {

    private final long readCount;
    private final long batchSubmitted;
    private final long batchOk;
    private final long batchFailed;
    private final long recordSuccess;
    private final long recordFailed;
    private final long recordBatchFailed;
    private final Map<String, Long> channelCounts;
    private final Map<String, Long> codeCounts;
    private final long elapsedMillis;
    private final boolean aborted;
    private final Throwable abortCause;

    BatchResult(long readCount, long batchSubmitted, long batchOk, long batchFailed,
                long recordSuccess, long recordFailed, long recordBatchFailed,
                Map<String, Long> channelCounts, Map<String, Long> codeCounts,
                long elapsedMillis, boolean aborted, Throwable abortCause) {
        this.readCount = readCount;
        this.batchSubmitted = batchSubmitted;
        this.batchOk = batchOk;
        this.batchFailed = batchFailed;
        this.recordSuccess = recordSuccess;
        this.recordFailed = recordFailed;
        this.recordBatchFailed = recordBatchFailed;
        this.channelCounts = channelCounts;
        this.codeCounts = codeCounts;
        this.elapsedMillis = elapsedMillis;
        this.aborted = aborted;
        this.abortCause = abortCause;
    }

    public long getReadCount() {
        return readCount;
    }

    public long getBatchSubmitted() {
        return batchSubmitted;
    }

    public long getBatchOk() {
        return batchOk;
    }

    public long getBatchFailed() {
        return batchFailed;
    }

    public long getRecordSuccess() {
        return recordSuccess;
    }

    public long getRecordFailed() {
        return recordFailed;
    }

    public long getRecordBatchFailed() {
        return recordBatchFailed;
    }

    /** 失败记录合计（记录级 + 批次级牵连）。 */
    public long getRecordFailedTotal() {
        return recordFailed + recordBatchFailed;
    }

    /** 各通道累计写入行数（key → 行数）。 */
    public Map<String, Long> getChannelCounts() {
        return channelCounts;
    }

    /** 状态码出现次数（code → 次数）。 */
    public Map<String, Long> getCodeCounts() {
        return codeCounts;
    }

    public long getElapsedMillis() {
        return elapsedMillis;
    }

    /** true 表示运行被提前停止（读取失败、写分片失败、外部 abort 等）。 */
    public boolean isAborted() {
        return aborted;
    }

    public Throwable getAbortCause() {
        return abortCause;
    }

    @Override
    public String toString() {
        return "BatchResult{read=" + readCount + ", ok=" + recordSuccess
                + ", failed=" + getRecordFailedTotal() + ", batches=" + batchSubmitted
                + ", channels=" + channelCounts + ", elapsed=" + elapsedMillis + "ms"
                + (aborted ? ", ABORTED: " + abortCause : "") + "}";
    }
}
