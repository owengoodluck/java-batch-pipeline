package com.example.batch;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 通道路由器：把「一条记录 + 它的处理结果」解析成一个**已注册**的通道 key。
 *
 * <p>路由规则刻意保持简单，只有两条：
 * <ol>
 *   <li><b>失败的记录一律进错误通道</b>——不需要任何配置，{@link RoutingProcessor#channelOf}
 *       也只对成功记录调用，因此"失败记录被路由进业务通道"这类歧义从模型上就不存在；</li>
 *   <li><b>成功的记录</b>：处理器实现了 {@link RoutingProcessor} → 用它给的 key；
 *       没实现或返回 {@code null} → 进 {@code success} 通道。</li>
 * </ol>
 *
 * <p>另外一道安全阀：<b>返回了未注册的 key，不会动态建通道</b>，而是兜底到错误通道并计数告警。
 * 允许"边跑边建通道"看起来更灵活，但代价是一个大小写错误或多余空格就会静默产出
 * 一组没人读的文件，且不报错——等对账时才发现少了数据。白名单 + 兜底 + 计数，
 * 能把这类事故压缩成一条明确的日志。
 *
 * <p>兜底记录往往"业务上是成功的"，如果按成功格式写进错误文件，错误文件里就会
 * 混入不同列数的行，下游解析直接错位。因此 {@link PartWriter} 会为它们合成一条
 * UNROUTED 失败结果，强制走错误格式。
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
    private final ConcurrentHashMap<String, AtomicLong> unregistered =
            new ConcurrentHashMap<String, AtomicLong>();
    /** 成功记录被显式指到错误通道的次数（该通道只收失败行，会被拨回 success）。 */
    private final AtomicLong successToError = new AtomicLong();

    ChannelRouter(PipelineConfig<T, P> config) {
        this.processor = config.getProcessor();
        this.channels = config.getChannels();
    }

    public Route resolve(int indexInBatch, T record, ItemResult<P> result) {
        final String requested;
        final String key;
        if (result.isSuccess()) {
            requested = (processor instanceof RoutingProcessor)
                    ? ((RoutingProcessor<T, P>) processor).channelOf(indexInBatch, record, result)
                    : null;
            if (requested == null) {
                key = OutputChannel.SUCCESS;
            } else if (OutputChannel.ERROR.equals(requested)) {
                // 错误通道只收失败行（同一个 RecordFormatter 只渲染失败结果）：
                // 成功记录指过来无法落盘，拨回 success 并一次性告警
                noteSuccessToError(indexInBatch, record);
                key = OutputChannel.SUCCESS;
            } else {
                key = requested;
            }
        } else {
            requested = null;
            key = OutputChannel.ERROR;
        }

        if (channels.containsKey(key)) {
            return new Route(key, requested, false);
        }
        noteUnregistered(key, indexInBatch, record);
        return new Route(OutputChannel.ERROR, key, true);
    }

    private void noteUnregistered(String key, int indexInBatch, T record) {
        long n = increment(unregistered, key);
        if (n == 1L) {
            // 只报首次，避免一个批次 100 条刷 100 行日志；总次数在汇总里给
            PipelineLog.warn("未注册的输出通道 \"%s\"（批次内下标 %d，记录 %s），已兜底到 \"%s\"。"
                            + "请检查配置声明与处理器返回值是否有拼写或空格差异。",
                    key, indexInBatch, String.valueOf(record), OutputChannel.ERROR);
        }
    }

    private void noteSuccessToError(int indexInBatch, T record) {
        long n = successToError.incrementAndGet();
        if (n == 1L) {
            PipelineLog.warn("成功记录被指到错误通道 \"%s\"（批次内下标 %d，记录 %s），"
                            + "已拨回 \"%s\" 通道。错误通道只承载失败行。",
                    OutputChannel.ERROR, indexInBatch, String.valueOf(record), OutputChannel.SUCCESS);
        }
    }

    private static long increment(ConcurrentHashMap<String, AtomicLong> map, String key) {
        AtomicLong counter = map.get(key);
        if (counter == null) {
            counter = new AtomicLong();
            AtomicLong prev = map.putIfAbsent(key, counter);
            if (prev != null) {
                counter = prev;
            }
        }
        return counter.incrementAndGet();
    }

    /** 未注册通道的命中次数快照（key → 次数）。 */
    public Map<String, Long> unregisteredSnapshot() {
        return snapshot(unregistered);
    }

    public long unregisteredTotal() {
        return total(unregistered);
    }

    /** 成功记录被指到错误通道的次数（已被自动拨回 success）。 */
    public long successToErrorTotal() {
        return successToError.get();
    }

    private static Map<String, Long> snapshot(ConcurrentHashMap<String, AtomicLong> map) {
        Map<String, Long> snapshot = new LinkedHashMap<String, Long>();
        for (Map.Entry<String, AtomicLong> e : map.entrySet()) {
            snapshot.put(e.getKey(), e.getValue().get());
        }
        return snapshot;
    }

    private static long total(ConcurrentHashMap<String, AtomicLong> map) {
        long total = 0L;
        for (AtomicLong v : map.values()) {
            total += v.get();
        }
        return total;
    }
}
