package com.example.batch;

/**
 * 批次级失败：整批调用没有拿到服务端结果（超时、连接失败、5xx、响应解析失败等）。
 *
 * <p>抛出后模板会把这一批的全部记录写入错误文件（级别 BATCH），错误码取自本异常，
 * 然后继续处理后续批次。
 */
public class BatchFailedException extends Exception {

    private static final long serialVersionUID = 1L;

    private final String errorCode;

    public BatchFailedException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public BatchFailedException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return errorCode;
    }
}
