package com.example.demo;

import com.example.batch.ItemResult;
import com.example.batch.RecordFormatter;

/**
 * 分组演示的行格式：成功行与错误行共用同一个 {@link RecordFormatter} 类型，
 * 由路由保证成功通道只看到成功记录、错误通道只看到失败记录。
 */
public final class DemoGroupFormatter {

    /** 成功行：分组键 \t 负载。 */
    public static final RecordFormatter<DemoGroupRecord, String> SUCCESS =
            (rec, res) -> rec.getKey() + "\t" + res.getPayload();

    /** 错误行：错误码 \t 错误信息 \t 分组键 \t 原始负载。 */
    public static final RecordFormatter<DemoGroupRecord, String> ERROR =
            (rec, res) -> res.getCode() + "\t" + res.getMessage()
                    + "\t" + rec.getKey() + "\t" + rec.getPayload();
}
