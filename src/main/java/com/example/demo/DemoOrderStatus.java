package com.example.demo;

/**
 * 订单状态枚举（演示用）。
 *
 * <p><b>刻意保留一个未注册通道的状态</b>：{@link #PARTIAL}（部分发货）的 channelKey
 * 是 {@code partial_shipped}，但 Demo 的配置里没有声明这个通道。这样能真实演示
 * "上游新增了一种状态、下游代码还没跟上"时会发生什么——它会被兜底到错误通道并告警，
 * 而不是无声无息地消失。
 *
 * <p>状态由订单号确定性推导，不用随机数：同样的输入必然得到同样的分布，
 * 这样才能在 CI 里断言精确条数。
 */
public enum DemoOrderStatus {

    PAID("paid", "已付款"),
    SHIPPED("shipped", "已发货"),
    COMPLETED("completed", "已完成"),
    CANCELLED("cancelled", "已取消"),
    /** 新状态：Demo 未注册对应通道，用于演示兜底。 */
    PARTIAL("partial_shipped", "部分发货");

    private final String channelKey;
    private final String label;

    DemoOrderStatus(String channelKey, String label) {
        this.channelKey = channelKey;
        this.label = label;
    }

    public String getChannelKey() {
        return channelKey;
    }

    public String getLabel() {
        return label;
    }

    /** 按订单号尾数分布状态，权重：已付款 40% / 已发货 30% / 已完成 15% / 已取消 10% / 部分发货 5%。 */
    public static DemoOrderStatus of(String orderId) {
        int bucket = lastNumber(orderId) % 100;
        if (bucket < 40) {
            return PAID;
        }
        if (bucket < 70) {
            return SHIPPED;
        }
        if (bucket < 85) {
            return COMPLETED;
        }
        if (bucket < 95) {
            return CANCELLED;
        }
        return PARTIAL;
    }

    /** 模拟"查不到这笔订单"：约 0.7% 的订单号命中。 */
    public static boolean isMissing(String orderId) {
        int n = lastNumber(orderId);
        return n > 0 && n % 137 == 0;
    }

    /** 取订单号中的数字部分（{@code ORD-000137} → 137）。 */
    static int lastNumber(String orderId) {
        if (orderId == null) {
            return 0;
        }
        int n = 0;
        boolean found = false;
        for (int i = 0; i < orderId.length(); i++) {
            char c = orderId.charAt(i);
            if (c >= '0' && c <= '9') {
                n = (n * 10 + (c - '0')) % 1000000;
                found = true;
            }
        }
        return found ? n : 0;
    }
}
