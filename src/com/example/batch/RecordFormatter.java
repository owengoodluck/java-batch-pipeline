package com.example.batch;

/**
 * 输出通道的行格式化器：把「一条记录 + 它的处理结果」变成输出文件的一行。
 *
 * <p>一个通道只收到一种结果：普通通道（如 success / paid）只收到成功记录，
 * 错误通道（key="error"）只收到失败记录。无论哪种，都通过同一个 {@code format}
 * 方法渲染——成功时读 {@code result.getPayload()}，失败时读 {@code result.getMessage()}。
 * 因此一个 formatter 既能当 lambda 直接写，也能复用到多个通道。
 *
 * <pre>
 * .channel("paid", file, (record, result) -&gt; record + "\t" + result.getCode())
 * </pre>
 *
 * @param <T> 原始记录类型
 * @param <P> 附加内容类型
 */
public interface RecordFormatter<T, P> {

    /** 一条记录（及其处理结果）→ 输出文件的一行。 */
    String format(T record, ItemResult<P> result);
}
