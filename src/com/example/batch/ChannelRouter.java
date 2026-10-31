package com.example.batch;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 通道路由器：把「一条记录 + 它的处理结果」解析成一个**已注册**的通道 key。
 *
 * <p>解析顺序：
 * <ol>
 *   <li>处理器实现了 {@link RoutingProcessor} → 用它给的 key；</li>
 *   <li>返回 {@code null} → 按结果状态走默认（成功 → {@code success}，失败 → {@code error}）；</li>
 *   <li>key 不在配置里 → 落错误通道，标记为 fallback，并计入「未注册路由」统计。</li>
 * </ol>
 *
 * <p>第三步是这套设计的安全阀。允许"边跑边建通道"看起来更灵活，但代价是
 * 一个大小写错误或多余空格就会静默产出一组没人读的文件，且不报错——等对账时才发现少了数据。
 * 白名单 + 兜底 + 计数，能把这类事故压缩成一条明确的日志。
 *
 * <p>fallback 必须被上层感知：这类记录往往"业务上是成功的"，如果按成功格式写进错误文件，
 * 错误文件里就会混入不同列数的行，下游解析直接错位。因此 {@link PartWriter} 会为它们
 * 合成一条 UNROUTED 失败结果，强制走错误格式。
 *
 * <p>线程安全：由多个消费者线程并发调用。
 */
public final class ChannelRouter<T, P> {

    /** 一次路由的结果。 */
    public static final class Route {

        private final String key;
        private final String requestedKey;
        private final boolean fallback;

        Route(String key, String requestedKey, boolean fallback) {
            this.key = key;
            this.requestedKey = requestedKey;
            this.fallback = fallback;
        }

        /** 实际落盘的通道 key（一定已注册）。 */
        public String getKey() {
            return key;
        }

        /** 处理器原本请求的 key；走默认规则时为 null。 */
        public String getRequestedKey() {
            return requestedKey;
        }

        /** true 表示请求了未注册的通道，已被兜底。 */
        public boolean isFallback() {
            return fallback;
        }
    }

    private final BatchProcessor<T, P> processor;
    private final Map<String, OutputChannel<T, P>> channels;
    private final String errorKey;
    private final ConcurrentHashMap<String, AtomicLong> unregistered =
            new ConcurrentHashMap<String, AtomicLong>();

    ChannelRouter(PipelineConfig<T, P> config) {
        this.processor = config.getProcessor();
        this.channels = config.getChannels();
        this.errorKey = config.getErrorChannelKey();
    }

    public Route resolve(int indexInBatch, T record, ItemResult<P> result) {
        String requested = null;
        if (processor instanceof RoutingProcessor) {
            requested = ((RoutingProcessor<T, P>) processor).channelOf(indexInBatch, record, result);
        }
        String key = requested;
        if (key == null) {
            key = result.isSuccess() ? OutputChannel.SUCCESS : OutputChannel.ERROR;
        }
        if (channels.containsKey(key)) {
            return new Route(key, requested, false);
        }
        noteUnregistered(key, indexInBatch, record);
        return new Route(errorKey, key, true);
    }

    private void noteUnregistered(String key, int indexInBatch, T record) {
        AtomicLong counter = unregistered.get(key);
        if (counter == null) {
            counter = new AtomicLong();
            AtomicLong prev = unregistered.putIfAbsent(key, counter);
            if (prev != null) {
                counter = prev;
            }
        }
        long n = counter.incrementAndGet();
        if (n == 1L) {
            // 只报首次，避免一个批次 100 条刷 100 行日志；总次数在汇总里给
            PipelineLog.warn("未注册的输出通道 \"%s\"（批次内下标 %d，记录 %s），已兜底到 \"%s\"。"
                            + "请检查配置声明与处理器返回值是否有拼写或空格差异。",
                    key, indexInBatch, String.valueOf(record), errorKey);
        }
    }

    /** 未注册通道的命中次数快照（key → 次数）。 */
    public Map<String, Long> unregisteredSnapshot() {
        Map<String, Long> snapshot = new java.util.LinkedHashMap<String, Long>();
        for (Map.Entry<String, AtomicLong> e : unregistered.entrySet()) {
            snapshot.put(e.getKey(), e.getValue().get());
        }
        return snapshot;
    }

    public long unregisteredTotal() {
        long total = 0L;
        for (AtomicLong v : unregistered.values()) {
            total += v.get();
        }
        return total;
    }
}
