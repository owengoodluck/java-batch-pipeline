package com.example.demo;

/**
 * 演示用记录：带一个分组键字段 {@link #key} 和负载 {@link #payload}。
 * 真实业务里，分组键可以是订单的「客户号 / 仓库 / 渠道」等任意字段。
 */
public final class DemoGroupRecord {

    private final String key;
    private final String payload;

    public DemoGroupRecord(String key, String payload) {
        this.key = key;
        this.payload = payload;
    }

    /** 分组键（演示里按它把记录归到同一组）。 */
    public String getKey() {
        return key;
    }

    /** 业务负载（这里只是原样透传的字符串）。 */
    public String getPayload() {
        return payload;
    }

    /** 从 "KEY\tPAYLOAD" 解析；非法行返回 null，由 reader 跳过。 */
    public static DemoGroupRecord fromLine(String line) {
        if (line == null) {
            return null;
        }
        int idx = line.indexOf('\t');
        if (idx <= 0) {
            return null;
        }
        return new DemoGroupRecord(line.substring(0, idx), line.substring(idx + 1));
    }
}
