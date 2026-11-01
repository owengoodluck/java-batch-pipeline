package com.example.batch;

/**
 * 单条记录的处理结果。状态码含义由使用方的 BatchProcessor 决定，模板不做解读。
 *
 * <p>只有「成功 / 失败」两种状态，用一个布尔位表达即可，不设枚举类型。
 *
 * @param <P> 附加内容类型（HTTP 响应里逐条返回的字段）
 */
public final class ItemResult<P> {

    private final boolean success;
    private final String code;
    private final P payload;
    private final String message;

    private ItemResult(boolean success, String code, P payload, String message) {
        this.success = success;
        this.code = code;
        this.payload = payload;
        this.message = message;
    }

    /** 成功：payload 携带服务端逐条返回的附加内容。 */
    public static <P> ItemResult<P> success(String code, P payload) {
        return new ItemResult<P>(true, code, payload, null);
    }

    /** 失败：message 说明业务上没通过的原因。 */
    public static <P> ItemResult<P> failed(String code, String message) {
        return new ItemResult<P>(false, code, null, message);
    }

    public boolean isSuccess() {
        return success;
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

    @Override
    public String toString() {
        return success ? "OK(" + code + ")" : "FAILED(" + code + ": " + message + ")";
    }
}
