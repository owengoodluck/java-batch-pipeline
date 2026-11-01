package com.example.demo;

import com.example.batch.BatchFailedException;
import com.example.batch.BatchProcessor;
import com.example.batch.ItemResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 模拟 HTTP 批量接口的处理器（BatchProcessor 的一个具体实现）。
 *
 * <p>真实项目里把 process() 内部替换成一次 HTTP 调用即可：
 * 请求体 = 这一批 100 条记录，响应体里逐条携带状态码与附加字段。
 *
 * <p>这里用随机数模拟三类结果，方便观察到分流效果：
 * <ul>
 *   <li>批次级失败：整批超时 / 5xx —— 抛 BatchFailedException，该批 100 条进错误文件（级别 BATCH）；</li>
 *   <li>记录级失败：单条业务不通过 —— 返回 FAILED 结果，进错误文件（级别 RECORD）；</li>
 *   <li>成功：返回服务端附加字段（如服务端流水号、受理状态）。</li>
 * </ul>
 */
public class DemoHttpProcessor implements BatchProcessor<String, String> {

    private static final String[] BATCH_ERRORS = {"HTTP_500", "TIMEOUT", "CONN_RESET"};
    private static final String[] RECORD_CODES = {"E1001", "E2002", "E3003"};
    private static final String[] RECORD_MSGS = {"余额不足", "数据校验未通过", "单据重复"};

    private final int batchFailPercent;
    private final int recordFailPercent;
    private final long seed;

    public DemoHttpProcessor(int batchFailPercent, int recordFailPercent, long seed) {
        this.batchFailPercent = batchFailPercent;
        this.recordFailPercent = recordFailPercent;
        this.seed = seed;
    }

    public List<ItemResult<String>> process(List<String> batch, long batchSeq)
            throws BatchFailedException {
        // 每个批次用「固定 seed + 批次号」派生独立随机数，保证同一输入每次运行结果完全一致，
        // 且各消费者线程之间互不干扰（不再共享一个 Random）。
        Random rng = new Random(seed ^ batchSeq);

        // 模拟网络耗时：80~220ms
        int cost = 80 + rng.nextInt(140);
        try {
            Thread.sleep(cost);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BatchFailedException("INTERRUPTED", "调用被中断", e);
        }

        // 批次级失败
        if (rng.nextInt(100) < batchFailPercent) {
            String code = BATCH_ERRORS[rng.nextInt(BATCH_ERRORS.length)];
            throw new BatchFailedException(code, "批量接口调用失败(" + code + ")，耗时 " + cost + "ms");
        }

        // 逐条结果
        List<ItemResult<String>> results = new ArrayList<ItemResult<String>>(batch.size());
        for (int i = 0; i < batch.size(); i++) {
            if (rng.nextInt(100) < recordFailPercent) {
                int k = rng.nextInt(RECORD_CODES.length);
                results.add(ItemResult.<String>failed(RECORD_CODES[k], RECORD_MSGS[k]));
            } else {
                // 模拟服务端逐条返回的附加内容：流水号 + 受理状态 + 服务端时间戳
                String payload = "SID-" + batchSeq + "-" + (i + 1)
                        + "|ACCEPTED|" + System.currentTimeMillis();
                results.add(ItemResult.<String>success("OK", payload));
            }
        }
        return results;
    }
}
