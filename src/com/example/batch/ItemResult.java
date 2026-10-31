package com.example.batch;

/**
 * 单条记录的处理结果。状态码含义由使用方的 BatchProcessor 决定，模板不做解读。
 *
 * @param <P> 附加内容类型（HTTP 响应里逐条返回的字段）
 */
public final class ItemResult<P> {

    private final OutcomeStatus status;
    private final String code;
    private final P payload;
    private final String message;

    private ItemResult(OutcomeStatus status, String code, P payload, String message) {
        this.status = status;
        this.code = code;
        this.payload = payload;
        this.message = message;
    }

    public static <P> ItemResult<P> success(String code, P payload) {
        return new ItemResult<P>(OutcomeStatus.SUCCESS, code, payload, null);
    }

    public static <P> ItemResult<P> failed(String code, String message) {
        return new ItemResult<P>(OutcomeStatus.FAILED, code, null, message);
    }

    public OutcomeStatus getStatus() {
        return status;
    }

    public boolean isSuccess() {
        return status == OutcomeStatus.SUCCESS;
    }

    public String getCode() {
        return code;
    }

    public P getPayload() {
        return payload;
    }

    public String getMessage() {
        return message;
    }
}
