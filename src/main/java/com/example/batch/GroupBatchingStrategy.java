package com.example.batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 内置分组攒批策略：每条记录按 {@link GroupKeyExtractor} 取出的键归到对应桶；
 * 某个桶攒满 batchSize 即作为一个<b>单组批次</b>提交——因此每次
 * {@link BatchProcessor#process(List, long)} 收到的批次<b>只含同一个分组键</b>（单组保证）。
 *
 * <p><b>内存模型（关键）</b>：任意时刻驻留 = 各桶「未满部分」之和 ≤ 组数 × (batchSize−1)，
 * 与输入总量 N 无关——所以本策略<b>无需把全部记录一次性读入内存</b>，
 * 这正是它不采用「先全量读入再分组」方案的根本理由（后者内存占用是 O(N)，可能 OOM）。
 *
 * <p><b>适用前提（硬约束）</b>：分组键的<b>不同值数量必须有界且较小（低基数）</b>，
 * 典型如租户 / 地区 / 渠道，几十到几百个。若分组键是高基数（如按用户 ID，可能上百万不同值），
 * 会出现大量「永远攒不满」的桶同时驻留，内存重新变成无界——高基数场景需另加「桶上限 + LRU 淘汰」
 * （未来增强），不在本实现范围。
 *
 * <p>行为细节：
 * <ul>
 *   <li>某键的桶被 flush 后若再有同键记录到达，会开一个新桶继续攒，仍保持单组语义；</li>
 *   <li>分组键为 null 时归入一个独立桶（多占一个桶，低基数下仍安全）；</li>
 *   <li>输入结束时 {@link #drainAll()} 把所有未满桶兜底 flush，尾批不会丢。</li>
 * </ul>
 */
public final class GroupBatchingStrategy<T> implements BatchingStrategy<T> {

    private final int batchSize;
    private final GroupKeyExtractor<T, ?> extractor;
    private final Map<Object, List<T>> buckets = new LinkedHashMap<Object, List<T>>();

    public GroupBatchingStrategy(int batchSize, GroupKeyExtractor<T, ?> extractor) {
        this.batchSize = batchSize;
        this.extractor = Objects.requireNonNull(extractor, "groupExtractor");
    }

    public void add(T record) throws IOException {
        Object key = extractor.extract(record);
        List<T> bucket = buckets.get(key);
        if (bucket == null) {
            bucket = new ArrayList<T>(batchSize);
            buckets.put(key, bucket);
        }
        bucket.add(record);
    }

    public boolean hasReadyBatch() {
        for (List<T> bucket : buckets.values()) {
            if (bucket.size() >= batchSize) {
                return true;
            }
        }
        return false;
    }

    public List<T> drainReadyBatch() {
        Object readyKey = null;
        for (Map.Entry<Object, List<T>> e : buckets.entrySet()) {
            if (e.getValue().size() >= batchSize) {
                readyKey = e.getKey();
                break;
            }
        }
        if (readyKey == null) {
            return null;
        }
        // 先取到 key 再移除，避免在 entry 迭代中直接改 map 触发 ConcurrentModificationException
        return buckets.remove(readyKey);
    }

    public List<List<T>> drainAll() {
        List<List<T>> all = new ArrayList<List<T>>(buckets.size());
        for (List<T> bucket : buckets.values()) {
            all.add(bucket);
        }
        buckets.clear();
        return all;
    }
}
