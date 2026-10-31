package com.example.batch;

import java.util.List;

/**
 * 批处理逻辑（SPI：每种文件类型提供自己的实现，通常是调用 HTTP 接口）。
 *
 * @param <T> 原始记录类型
 * @param <P> 附加内容类型
 */
public interface BatchProcessor<T, P> {

    /**
     * 处理一批记录。
     *
     * @param batch    一批记录（最后一批可能不足 batchSize）
     * @param batchSeq 批次序号（从 1 开始，与文件中的位置顺序一致）
     * @return 与入参 batch 一一对应、顺序一致的结果列表
     * @throws BatchFailedException 整批调用失败时抛出，该批全部记录会标记为 BATCH 级错误
     */
    List<ItemResult<P>> process(List<T> batch, long batchSeq) throws BatchFailedException;
}
