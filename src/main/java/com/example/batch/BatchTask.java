package com.example.batch;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * 单批任务：一个批次 = 一次批量调用 + 一次分流写入。
 *
 * <p>失败策略（按设计定稿）：
 * <ul>
 *   <li>批次调用失败（BatchFailedException / 其他异常）→ 整批写入错误通道，后续批次继续；</li>
 *   <li>写分片失败 = 兜底失效 → 触发全局停止（不能静默丢数据）；</li>
 *   <li>致命错误（Error，如 OOM）→ 触发全局停止。</li>
 * </ul>
 *
 * <p>无论走哪条路径，finally 都归还名额，这是背压不泄漏的唯一保障。
 */
public final class BatchTask<T, P> implements Runnable {

    private final FileBatchPipeline<T, P> pipeline;
    private final PipelineConfig<T, P> config;
    private final PipelineStats stats;
    private final PartWriter<T, P> partWriter;
    private final Semaphore semaphore;
    private final long batchSeq;
    private final List<T> batch;

    BatchTask(FileBatchPipeline<T, P> pipeline, PipelineConfig<T, P> config, PipelineStats stats,
              PartWriter<T, P> partWriter, Semaphore semaphore, long batchSeq, List<T> batch) {
        this.pipeline = pipeline;
        this.config = config;
        this.stats = stats;
        this.partWriter = partWriter;
        this.semaphore = semaphore;
        this.batchSeq = batchSeq;
        this.batch = batch;
    }

    public void run() {
        try {
            processBatch();
        } catch (IOException e) {
            // 兜底失效：连错误文件都写不了，继续跑会静默丢数据
            PipelineLog.error("批次 #%d 写入分片失败，触发全局停止: %s", batchSeq, e.toString());
            pipeline.abort(e);
        } catch (Error e) {
            PipelineLog.error("批次 #%d 发生致命错误，触发全局停止: %s", batchSeq, e.toString());
            pipeline.abort(e);
        } finally {
            semaphore.release();
        }
    }

    private void processBatch() throws IOException {
        List<ItemResult<P>> results;
        try {
            results = config.getProcessor().process(batch, batchSeq);
        } catch (BatchFailedException e) {
            handleBatchFailed(e.getErrorCode(), e.getMessage());
            return;
        } catch (Exception e) {
            // processor 抛出的其他异常同样降级为该批失败，不拖垮整个文件
            handleBatchFailed("UNKNOWN", String.valueOf(e.getMessage()));
            return;
        }

        // 防御：结果与批次条数不一致时按整批失败处理，避免记录不明丢失
        if (results == null || results.size() != batch.size()) {
            handleBatchFailed("BAD_RESPONSE",
                    "返回结果条数(" + (results == null ? 0 : results.size())
                            + ")与批次(" + batch.size() + ")不符");
            return;
        }

        Map<String, Integer> counted = partWriter.writeResults(batchSeq, batch, results);
        stats.addBatchOk();
        countResults(results);
        for (Map.Entry<String, Integer> e : counted.entrySet()) {
            stats.addChannel(e.getKey(), e.getValue());
        }
        PipelineLog.info("批次 #%d 完成: %s", batchSeq, describeChannels(counted));
    }

    private void handleBatchFailed(String errorCode, String message) throws IOException {
        int n = partWriter.writeBatchFailed(batchSeq, batch, errorCode, message);
        stats.addBatchFailed();
        stats.addChannel(config.getErrorChannelKey(), n);
        for (int i = 0; i < n; i++) {
            stats.addRecordBatchFailed();
        }
        stats.addCode(errorCode);
        PipelineLog.warn("批次 #%d 整批失败: code=%s msg=%s（%d 条已写入错误通道 %s）",
                batchSeq, errorCode, message, n, config.getErrorChannelKey());
    }

    /** 按通道声明顺序拼一段稳定的描述，避免日志随线程调度抖动。 */
    private String describeChannels(Map<String, Integer> counted) {
        StringBuilder sb = new StringBuilder();
        for (String key : config.getChannels().keySet()) {
            Integer n = counted.get(key);
            if (n == null || n.intValue() == 0) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(" / ");
            }
            sb.append(key).append(' ').append(n).append(" 条");
        }
        return sb.length() == 0 ? "无写入" : sb.toString();
    }

    private void countResults(List<ItemResult<P>> results) {
        for (ItemResult<P> r : results) {
            stats.addCode(r.getCode());
            if (r.isSuccess()) {
                stats.addRecordSuccess();
            } else {
                stats.addRecordFailed();
            }
        }
    }
}
