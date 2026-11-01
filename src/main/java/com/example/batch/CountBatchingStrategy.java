package com.example.batch;

import java.util.ArrayList;
import java.util.List;

/**
 * 默认攒批策略：等价于「按数量切批」的原始行为——记录按到达顺序攒入一个 pending 列表，
 * 达到 batchSize 即整体作为一个批次提交。{@link PipelineConfig.Builder#groupBy} 未配置时使用本策略，
 * 行为与不分组时完全不变（向后兼容）。
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

    public boolean hasReadyBatch() {
        return pending.size() >= batchSize;
    }

    public List<T> drainReadyBatch() {
        List<T> batch = pending;
        pending = new ArrayList<T>();
        return batch;
    }

    public List<List<T>> drainAll() {
        List<List<T>> all = new ArrayList<List<T>>(1);
        if (!pending.isEmpty()) {
            all.add(pending);
            pending = new ArrayList<T>();
        }
        return all;
    }
}
