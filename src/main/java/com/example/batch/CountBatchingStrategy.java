package com.example.batch;

import java.util.ArrayList;
import java.util.List;

/**
 * 默认攒批策略：记录按到达顺序攒入一个 pending 列表，达到 batchSize 即整体作为一个批次提交。
 * 这是 {@link PipelineConfig.Builder#groupBy} 未配置时的标准成批方式；启用分组后由
 * {@link GroupBatchingStrategy} 接管，二者都实现同一个 {@link BatchingStrategy} 契约。
 */
public final class CountBatchingStrategy<T> implements BatchingStrategy<T> {

    private final int batchSize;
    private List<T> pending = new ArrayList<T>();

    public CountBatchingStrategy(int batchSize) {
        this.batchSize = batchSize;
    }

    public void add(T record) {
        pending.add(record);
    }

    /**
     * 攒满多少就交出多少个整批，每批长度<b>恰好是 batchSize</b>，余量留在 pending 里等下次或 EOF。
     * 这样即便调用方的取批节奏放慢，也不会交出超长批次。
     */
    public List<List<T>> drainReadyBatches() {
        List<List<T>> ready = new ArrayList<List<T>>();
        while (pending.size() >= batchSize) {
            List<T> batch = new ArrayList<T>(pending.subList(0, batchSize));
            pending.subList(0, batchSize).clear();
            ready.add(batch);
        }
        return ready;
    }

    public List<List<T>> drainAll() {
        List<List<T>> all = new ArrayList<List<T>>(1);
        if (!pending.isEmpty()) {
            all.add(pending);
            pending = new ArrayList<T>(batchSize);
        }
        return all;
    }
}
