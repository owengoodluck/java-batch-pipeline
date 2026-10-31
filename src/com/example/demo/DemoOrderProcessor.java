package com.example.demo;

import com.example.batch.BatchFailedException;
import com.example.batch.ItemResult;
import com.example.batch.RoutingProcessor;

import java.util.ArrayList;
import java.util.List;

/**
 * 按订单状态分流的批处理器：批量查订单状态，把结果路由到不同输出通道。
 *
 * <p>这是 {@link RoutingProcessor} 的参考实现，对应真实场景：
 * <pre>
 * 输入：订单号列表
 * 处理：一次批量调用查询订单状态（这里用确定性模拟代替 HTTP）
 * 输出：已付款 / 已发货 / 已完成 / 已取消 各一个文件，
 *       查询失败与"未注册状态"进错误文件
 * </pre>
 *
 * <p>两个细节值得注意：
 * <ul>
 *   <li><b>channelOf 只做映射，不重新查库</b>。它拿到的 {@code result.getPayload()}
 *       就是 process() 里查到的状态，直接映射即可。若在这里再调一次接口，会得到
 *       与落盘内容不一致的状态（查询两次之间状态可能已变），且性能翻倍。</li>
 *   <li><b>行内带查询时刻</b>。状态是查询那一刻的快照，落盘后可能已经变了；
 *       没有时间戳，事后无法解释"为什么这单在已付款文件里"。</li>
 * </ul>
 */
public class DemoOrderProcessor implements RoutingProcessor<String, String> {

    /** 真实项目里应替换为实际查询时刻；Demo 用常量保证结果可复现。 */
    private static final String QUERY_TIME = "2026-10-31T22:00:00+08:00";

    private static final char SEP = '|';

    public List<ItemResult<String>> process(List<String> batch, long batchSeq)
            throws BatchFailedException {
        simulateLatency(batchSeq);

        List<ItemResult<String>> results = new ArrayList<ItemResult<String>>(batch.size());
        for (String line : batch) {
            String orderId = firstField(line);
            if (DemoOrderStatus.isMissing(orderId)) {
                results.add(ItemResult.<String>failed("ORDER_NOT_FOUND", "订单不存在: " + orderId));
                continue;
            }
            DemoOrderStatus status = DemoOrderStatus.of(orderId);
            // payload 里带状态文案与查询时刻，formatter 直接落盘
            results.add(ItemResult.<String>success(status.name(),
                    status.getLabel() + SEP + QUERY_TIME));
        }
        return results;
    }

    public String channelOf(int indexInBatch, String record, ItemResult<String> result) {
        if (!result.isSuccess()) {
            // 返回 null 表示走默认规则 → 失败记录进错误通道
            return null;
        }
        // 用 code 而不是 payload：code 是稳定标识（PAID/SHIPPED...），payload 里是给人看的中文文案，
        // 拿文案去映射通道，改一次文案就会全线失配。
        String statusName = result.getCode();
        try {
            return DemoOrderStatus.valueOf(statusName).getChannelKey();
        } catch (RuntimeException e) {
            // 状态名不认识：交给内核兜底，它会落错误通道并计数
            return "unrecognized_status";
        }
    }

    /** 模拟批量查询耗时。用批次号派生而不是随机数，保证同一输入每次运行耗时一致。 */
    private void simulateLatency(long batchSeq) throws BatchFailedException {
        long cost = 40L + (batchSeq % 5) * 10L;
        try {
            Thread.sleep(cost);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BatchFailedException("INTERRUPTED", "查询被中断", e);
        }
    }

    private static String firstField(String line) {
        if (line == null) {
            return "";
        }
        int sep = line.indexOf('|');
        return sep < 0 ? line : line.substring(0, sep);
    }
}
