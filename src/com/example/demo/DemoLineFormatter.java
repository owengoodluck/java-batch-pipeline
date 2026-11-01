package com.example.demo;

import com.example.batch.ItemResult;
import com.example.batch.RecordFormatter;

/**
 * Demo 的输出行格式（平铺制表符）。
 *
 * <p>成功行（所有普通通道共用，见 {@link #SUCCESS_TSV}）：
 * <pre>原始记录 \t 状态码 \t 附加内容</pre>
 *
 * <p>失败行（错误通道，见 {@link #ERROR_TSV}）：
 * <pre>错误码 \t 错误信息 \t 原始记录</pre>
 *
 * <p>换成别的文件类型时，提供一个 {@link RecordFormatter} 即可，内核不需要动。
 * 同一个 formatter 类型既服务成功通道也服务错误通道——只不过被绑到错误通道的那个
 * 实例只会收到失败结果，读 {@code result.getMessage()} 即可。
 */
public final class DemoLineFormatter implements RecordFormatter<String, String> {

    public static final String SEP = "\t";

    /** 成功行的格式：普通通道收到的只有成功记录。 */
    public static final RecordFormatter<String, String> SUCCESS_TSV =
            (record, result) -> record + SEP + result.getCode() + SEP + result.getPayload();

    /** 错误行的格式：错误通道收到的只有失败记录，读 code / message。 */
    public static final RecordFormatter<String, String> ERROR_TSV =
            (record, result) -> result.getCode() + SEP
                    + (result.getMessage() == null ? "" : result.getMessage()) + SEP + record;

    @Override
    public String format(String record, ItemResult<String> result) {
        return result.isSuccess() ? SUCCESS_TSV.format(record, result) : ERROR_TSV.format(record, result);
    }
}
