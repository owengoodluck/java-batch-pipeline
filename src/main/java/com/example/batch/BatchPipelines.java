package com.example.batch;

/**
 * 一行式入口：装配好 {@link PipelineConfig} 后，直接 {@code BatchPipelines.run(config)}。
 *
 * <p>需要在中途 abort 或查询运行中状态时，再改用实例形态：
 * <pre>
 * BatchPipeline&lt;T, P&gt; pipeline = new BatchPipeline&lt;&gt;(config);
 * BatchResult result = pipeline.run();      // 内部其他线程可调 pipeline.abort(...)
 * </pre>
 */
public final class BatchPipelines {

    private BatchPipelines() {
    }

    /** 装配 → 运行 → 返回统计快照，一步到位。 */
    public static <T, P> BatchResult run(PipelineConfig<T, P> config) throws PipelineExecutionException {
        return new BatchPipeline<T, P>(config).run();
    }
}
