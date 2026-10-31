package com.example.demo;

import com.example.batch.ItemResult;
import com.example.batch.LineFormatter;

/**
 * Demo 的输出行格式化器（平铺制表符）。
 *
 * <p>主输出文件（只含成功记录）：
 * <pre>原始记录 \t 状态码 \t 附加内容</pre>
 *
 * <p>错误文件（含失败记录，带级别列）：
 * <pre>原始记录 \t 级别(RECORD|BATCH) \t 错误码 \t 错误信息</pre>
 *
 * <p>换成别的文件类型时，实现一个新的 LineFormatter 即可，内核不需要动。
 */
public class DemoLineFormatter implements LineFormatter<String, String> {

    public static final String SEP = "\t";

    public String formatSuccess(String record, ItemResult<String> result) {
        return record + SEP + result.getCode() + SEP + result.getPayload();
    }

    public String formatRecordFailed(String record, ItemResult<String> result) {
        return record + SEP + "RECORD" + SEP + result.getCode() + SEP + result.getMessage();
    }

    public String formatBatchFailed(String record, String errorCode, String message) {
        return record + SEP + "BATCH" + SEP + errorCode + SEP + message;
    }
}
