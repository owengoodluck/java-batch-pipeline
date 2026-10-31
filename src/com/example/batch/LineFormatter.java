package com.example.batch;

/**
 * 输出行格式化器（SPI：不同文件类型可以有完全不同的输出格式）。
 *
 * <p>主输出文件只写入成功行；错误文件写入记录级失败行与批次级失败行（带级别列）。
 *
 * @param <T> 原始记录类型
 * @param <P> 附加内容类型
 */
public interface LineFormatter<T, P> {

    /** 成功记录 → 主输出文件的一行。 */
    String formatSuccess(T record, ItemResult<P> result);

    /** 记录级失败 → 错误文件的一行（建议带 RECORD 级别标识）。 */
    String formatRecordFailed(T record, ItemResult<P> result);

    /** 批次级失败 → 错误文件的一行（建议带 BATCH 级别标识）。 */
    String formatBatchFailed(T record, String errorCode, String message);
}
