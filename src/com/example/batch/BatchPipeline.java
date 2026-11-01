package com.example.batch;

/**
 * 一行式入口：装配好 {@link PipelineConfig} 后，直接 {@code BatchPipeline.run(config)}。
 *
 * <p>需要在中途 abort 或查询运行中状态时，再改用实例形态：
 * <pre>
 * FileBatchPipeline&lt;T, P&gt; pipeline = new FileBatchPipeline&lt;&gt;(config);
 * BatchResult result = pipeline.run();      // 内部其他线程可调 pipeline.abort(...)
 * </pre>
 */
public final class BatchPipeline {

    private BatchPipeline() {
    }

    /** 装配 → 运行 → 返回统计快照，一步到位。 */
    public static <T, P> BatchResult run(PipelineConfig<T, P> config) throws PipelineExecutionException {
        return new FileBatchPipeline<T, P>(config).run();
    }
}
