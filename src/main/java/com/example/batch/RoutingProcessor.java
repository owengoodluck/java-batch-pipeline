package com.example.batch;

/**
 * 可按结果分流到不同输出通道的批处理器（{@link BatchProcessor} 的可选扩展）。
 *
 * <p>没实现这个接口的处理器行为完全不变：成功进 {@code success}，失败进 {@code error}。
 * 只有需要"不同结果写不同文件"时才实现它，属于增量能力。
 *
 * <p><b>典型场景</b>：批量查订单状态，按 已付款 / 已发货 / 已取消 / 已完成 分文件落盘。
 *
 * <p><b>约定</b>：
 * <ul>
 *   <li><b>只对成功记录调用</b>：失败的记录一律进错误通道，不需要（也无法）在这里路由；
 *       因此实现里不必再写 {@code if (!result.isSuccess()) ...} 这样的分支；</li>
 *   <li>返回 {@code null} 表示"进默认的 success 通道"；</li>
 *   <li>返回值必须是 {@link PipelineConfig} 里声明过的 key（错误通道 key 除外——
 *       成功记录指到错误通道会被拨回 success 并告警，因为错误通道只收失败行）；</li>
 *   <li>返回未声明的 key 会被兜底到 {@code error} 通道并累计告警，不会动态新建文件；</li>
 *   <li>实现必须线程安全：多个消费者线程会并发调用，不同批次之间没有 happens-before 关系。</li>
 * </ul>
 *
 * @param <T> 原始记录类型
 * @param <P> 附加内容类型
 */
public interface RoutingProcessor<T, P> extends BatchProcessor<T, P> {

    /**
     * 决定这条**成功**记录写往哪个通道。
     *
     * @param indexInBatch 本条记录在本批次内的下标（0 起），与 {@code process} 返回的列表对齐
     * @param record       原始记录
     * @param result       该记录的处理结果（保证为成功结果）
     * @return 通道 key；{@code null} 表示进默认的 success 通道
     */
    String channelOf(int indexInBatch, T record, ItemResult<P> result);
}
