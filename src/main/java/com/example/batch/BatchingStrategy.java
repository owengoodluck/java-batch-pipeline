package com.example.batch;

import java.util.List;

/**
 * 「如何成批」的策略。把 {@link BatchPipeline} 生产者里「攒批」这一步抽象成可替换的策略，
 * 默认实现按数量攒批（{@link CountBatchingStrategy}），分组场景用 {@link GroupBatchingStrategy}。
 *
 * <p>取批是一次调用、一次遍历：实现内部自行判断哪些批次达到 flush 阈值，一次全部返回。
 * 不设「先问有没有、再取一个」的两段式入口，也不以 null 表示「无就绪批次」——
 * 那样会把正确性押在调用方与实现之间的一致性约定上，实现一多就容易互相矛盾。
 *
 * <p>框架保证：返回的每个 {@code List<T>} 都是一个完整批次，交给
 * {@link BatchProcessor#process(List, long)} 时长度与顺序都自洽，
 * 因此下游契约与「不分组」时完全一致——下游代码无需感知自己收到的是「数量批」还是「单组批」。
 */
public interface BatchingStrategy<T> {

    /** 生产者每读到一条记录就喂进来（分组场景会在此刻按键入桶）。 */
    void add(T record);

    /**
     * 取出当前所有达到 flush 阈值的批次，取走后这批数据不再由本策略持有。
     *
     * @return 就绪批次列表，没有就绪时返回空列表
     */
    List<List<T>> drainReadyBatches();

    /**
     * 输入结束时取出所有未满的剩余批次，防止尾批丢失。取走后本策略不再持有任何数据。
     *
     * @return 剩余批次列表，没有剩余时返回空列表；顺序按实现内部的桶遍历顺序
     */
    List<List<T>> drainAll();
}
