package com.example.demo;

import com.example.batch.BatchPipeline;
import com.example.batch.BatchResult;
import com.example.batch.PipelineConfig;
import com.example.batch.PipelineLog;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

/**
 * 多通道分流示例：1000 条订单按查询到的状态写进 4 个业务文件 + 1 个错误文件。
 *
 * <pre>
 * 输入 input/orders.txt
 *   ORD-000001|CUST-00001|10.00
 *   ...
 *
 * 输出 output/
 *   order.paid        已付款
 *   order.shipped     已发货
 *   order.completed   已完成
 *   order.cancelled   已取消
 *   order.error       查询失败 + 未注册状态（部分发货）兜底
 * </pre>
 *
 * <p>"部分发货"在 {@link DemoOrderStatus} 里存在，但这里**故意不注册**对应通道，
 * 用来演示上游新增状态、下游未跟进时会被兜底并告警，而不是静默丢数据。
 */
public class DemoOrderMain {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static final String[] OUTPUT_FILES = {
            "order.paid", "order.shipped", "order.completed", "order.cancelled", "order.error"
    };

    public static void main(String[] args) throws Exception {
        File workDir = new File(args.length > 0 ? args[0] : "order-demo");
        int total = args.length > 1 ? Integer.parseInt(args[1]) : 1000;

        File input = new File(workDir, "input/orders.txt");
        File partsDir = new File(workDir, "parts");
        File outDir = new File(workDir, "output");

        generateOrders(input, total);

        PipelineConfig<String, String> config = PipelineConfig.<String, String>builder()
                .reader(new DemoLineReader(input, UTF8))
                .processor(new DemoOrderProcessor())
                .batchSize(100)
                .consumerThreads(2)
                .partsDir(partsDir)
                // Demo 保留分片目录，便于观察"每个通道一个子目录"的物理结构
                .deletePartsOnSuccess(false)
                // 普通通道只收成功记录：一个 lambda 就是一个通道的格式
                .channel("paid", new File(outDir, "order.paid"), DemoLineFormatter.SUCCESS_TSV)
                .channel("shipped", new File(outDir, "order.shipped"), DemoLineFormatter.SUCCESS_TSV)
                .channel("completed", new File(outDir, "order.completed"), DemoLineFormatter.SUCCESS_TSV)
                .channel("cancelled", new File(outDir, "order.cancelled"), DemoLineFormatter.SUCCESS_TSV)
                // 错误通道（兜底）：查询失败与未注册状态兜底都落这里
                .errorChannel(new File(outDir, "order.error"), DemoLineFormatter.ERROR_TSV)
                .build();

        BatchResult result = BatchPipeline.run(config);
        PipelineLog.info("运行结果: %s", result);

        report(workDir, total);
    }

    private static void report(File workDir, int total) throws IOException {
        File outDir = new File(workDir, "output");
        PipelineLog.info("---------- 输出文件明细 ----------");
        long sum = 0L;
        for (String name : OUTPUT_FILES) {
            File f = new File(outDir, name);
            long lines = countLines(f);
            sum += lines;
            PipelineLog.info("%-18s %5d 行%s", name, lines, f.exists() ? "" : "   <未生成>");
        }
        PipelineLog.info("%-18s %5d 条输入", "合计", total);
        if (sum == total) {
            PipelineLog.info("守恒成立：每条记录恰好落一个通道（%d = %d）", sum, total);
        } else {
            PipelineLog.warn("守恒失败：输出 %d 行 ≠ 输入 %d 条", sum, total);
        }

        PipelineLog.info("---------- order.error 前 3 行 ----------");
        File err = new File(outDir, "order.error");
        if (err.exists()) {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(new FileInputStream(err), UTF8));
            try {
                String line;
                int n = 0;
                while ((line = reader.readLine()) != null && n < 3) {
                    PipelineLog.info("%s", line);
                    n++;
                }
            } finally {
                reader.close();
            }
        }

        PipelineLog.info("---------- 分片目录 parts/ ----------");
        File[] channelDirs = new File(workDir, "parts").listFiles();
        if (channelDirs == null) {
            PipelineLog.info("（已清理）");
            return;
        }
        java.util.Arrays.sort(channelDirs);
        for (File dir : channelDirs) {
            if (!dir.isDirectory()) {
                continue;
            }
            File[] parts = dir.listFiles();
            PipelineLog.info("%-14s %2d 个分片", dir.getName() + "/", parts == null ? 0 : parts.length);
        }
    }

    /** 生成确定性的订单数据：同样的 total 每次产生完全相同的文件。 */
    private static void generateOrders(File file, int total) throws IOException {
        File parent = file.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
            throw new IOException("无法创建输入目录: " + parent);
        }
        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file, false), UTF8));
        try {
            for (int i = 1; i <= total; i++) {
                String orderId = String.format("ORD-%06d", i);
                String custId = String.format("CUST-%05d", (i % 500) + 1);
                String amount = String.format("%.2f", 10.0d + (i % 990) * 0.37d);
                writer.write(orderId + "|" + custId + "|" + amount);
                writer.write("\n");
            }
        } finally {
            writer.close();
        }
        PipelineLog.info("已生成输入文件: %s（%d 条）", file.getPath(), total);
    }

    private static long countLines(File file) throws IOException {
        if (!file.exists()) {
            return 0L;
        }
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), UTF8));
        long n = 0L;
        try {
            while (reader.readLine() != null) {
                n++;
            }
        } finally {
            reader.close();
        }
        return n;
    }
}
