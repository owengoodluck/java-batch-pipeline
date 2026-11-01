package com.example.batch;

import java.io.IOException;
import java.util.List;

/**
 * 「如何成批」的策略。把 {@link FileBatchPipeline} 生产者里「攒批」这一步抽象成可替换的策略，
 * 默认实现按数量攒批（{@link CountBatchingStrategy}），分组场景用 {@link GroupBatchingStrategy}。
 *
 * <p>框架保证：每次 {@link #drainReadyBatch()} / {@link #drainAll()} 返回的是一个完整的
 * {@code List<T>}（一个批次），交给 {@link BatchProcessor#process(List, long)} 时长度与顺序都自洽，
 * 因此下游契约与「不分组」时完全一致——下游代码无需感知自己收到的是「数量批」还是「单组批」。
 */
public interface BatchingStrategy<T> {

    /** 生产者每读到一条记录就喂进来（分组场景会在此刻按键入桶）。 */
    void add(T record) throws IOException;

    /** 是否已有至少一个批次达到 flush 阈值、可以提交。 */
    boolean hasReadyBatch();

    /**
     * 取出一个就绪批次。调用方须先确认 {@link #hasReadyBatch()} 为 true。
     * 返回的列表归调用方所有，本策略不再持有它（该桶随即清空/移除）。
     */
    List<T> drainReadyBatch();

    /** 输入结束时取出所有未满的剩余批次，防止尾批丢失。返回顺序按桶的遍历顺序。 */
    List<List<T>> drainAll();
}
