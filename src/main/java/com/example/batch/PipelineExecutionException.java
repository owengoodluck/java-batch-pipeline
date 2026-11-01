package com.example.batch;

/** 流水线执行失败（读取异常、兜底失效、被中断等）时抛出，此时 {@link BatchPipeline} 的统计仍可查询。 */
public class PipelineExecutionException extends Exception {

    private static final long serialVersionUID = 1L;

    public PipelineExecutionException(String message) {
        super(message);
    }

    public PipelineExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
