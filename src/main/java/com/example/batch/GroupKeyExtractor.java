package com.example.batch;

import java.io.IOException;

/**
 * 从一条记录提取「分组键」。允许抛 IO 异常，与 {@link RecordReader} 的契约一致。
 *
 * <p>用于内置分组批处理：框架每读到一条记录，先用它取出分组键，再按键归到对应的桶；
 * 某个桶攒满 {@code batchSize} 条时，整桶作为一个「单组批次」提交给 {@link BatchProcessor}。
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
     * @throws IOException 读取相关的异常可在此抛出让框架统一处理
     */
    K extract(T record) throws IOException;
}
