package com.example.batch;

/**
 * 从一条记录提取「分组键」。
 *
 * <p>用于内置分组批处理：框架每读到一条记录，先用它取出分组键，再按键归到对应的桶；
 * 某个桶攒满 {@code batchSize} 条时，整桶作为一个「单组批次」提交给 {@link BatchProcessor}。
 *
 * <p>本接口<b>不声明</b> {@code throws IOException}：分组键取自已经读出来的记录，
 * 通常是纯粹的取字段/计算，与读取无关。同类 SPI {@link RoutingProcessor#channelOf} 同样不抛受检异常，
 * 两者契约保持一致，使用方也不必为了一个取值函数去处理 IO 异常。
 * 个别实现若确实涉及 IO，请自行包装为 {@link java.io.UncheckedIOException} 等运行时异常。
 *
 * @param <T> 记录类型
 * @param <K> 分组键类型
 */
@FunctionalInterface
public interface GroupKeyExtractor<T, K> {

    /**
     * 取出这条记录的分组键（如订单的客户号、仓库、渠道等字段）。
     *
     * @param record 当前记录（不会为 null）
     * @return 分组键；返回 null 也会被接受，归入一个独立的 null 桶
     */
    K extract(T record);
}
