package com.example.demo;

import com.example.batch.BatchPipeline;
import com.example.batch.BatchResult;
import com.example.batch.PipelineConfig;
import com.example.batch.PipelineExecutionException;
import com.example.batch.PipelineLog;
import com.example.batch.RecordReader;
import com.example.batch.RecordReaders;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;

/**
 * 可运行 Demo：读一个文本文件 → 每 100 条一批 → 多线程调"批量 HTTP 接口"
 * → 成功记录合并成主输出文件，失败记录合并成错误文件。
 *
 * <p>运行：java com.example.demo.DemoMain [工作目录] [reader|adapter]
 */
public class DemoMain {

    private static final Charset CS = Charset.forName("UTF-8");
    private static final int TOTAL_LINES = 1050;   // 故意不是 100 的整数倍，用于验证尾批
    private static final int BATCH_SIZE = 100;
    private static final int CONSUMER_THREADS = 4;
    private static final int BUFFER_BATCHES = 1;   // 在途上限 = 4 + 1 = 5

    public static void main(String[] args) throws Exception {
        File base = new File(args.length > 0 ? args[0] : "demo-run");
        String mode = args.length > 1 ? args[1] : "reader";

        File input = new File(base, "input/sample.txt");
        File outputDir = new File(base, "output");
        File partsDir = new File(base, "parts");

        generateInputIfAbsent(input, TOTAL_LINES);

        RecordReader<String> reader = createReader(input, mode);

        // 业务方只需提供：processor（真实调用）+ 两种 formatter（输出格式）
        DemoHttpProcessor processor = new DemoHttpProcessor(3, 5, 42L); // 3% 批次失败 / 5% 记录失败

        PipelineConfig<String, String> config = PipelineConfig.<String, String>builder()
                .reader(reader)
                .processor(processor)
                .batchSize(BATCH_SIZE)
                .consumerThreads(CONSUMER_THREADS)
                .bufferBatches(BUFFER_BATCHES)
                .awaitTerminationSeconds(60L)
                .charset(CS)
                .partsDir(partsDir)
                // 没实现 RoutingProcessor：成功记录自动进 success 通道，失败记录进 error 通道。
                // createEmpty(true)：无记录时也生成空文件，保持下游"固定文件集"的读法。
                .channel("success", new File(outputDir, "sample.success"),
                        DemoLineFormatter.SUCCESS_TSV, true)
                .errorChannel(new File(outputDir, "sample.error"),
                        DemoLineFormatter.ERROR_TSV, true)
                .build();

        PipelineLog.info("输入文件: %s（共 %d 行）", input.getAbsolutePath(), TOTAL_LINES);
        PipelineLog.info("读取方式: %s", "adapter".equalsIgnoreCase(mode) ? "适配器包装 read() 工具类"
                : "专用 RecordReader 实现");

        try {
            BatchResult result = BatchPipeline.run(config);
            PipelineLog.info("运行结果: %s", result);
        } catch (PipelineExecutionException e) {
            PipelineLog.error("流水线未正常完成: %s", e.getMessage());
            return;
        }

        preview(new File(outputDir, "sample.success"), "主输出文件 sample.success（成功记录）", 5);
        preview(new File(outputDir, "sample.error"), "错误文件 sample.error（失败记录）", 5);
    }

    // ------------------------------------------------------------ 两种读取接入方式

    private static RecordReader<String> createReader(File input, String mode) throws IOException {
        if ("adapter".equalsIgnoreCase(mode)) {
            // 方式 B：手上已有 read() 形态的工具类时，用适配器一行接入（原工具类不用改）
            final BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(input), CS));
            return RecordReaders.fromReadFunction(new RecordReaders.ExSupplier<String>() {
                public String get() throws java.io.IOException {
                    return br.readLine();
                }
            });
        }
        // 方式 A：专用实现（本 Demo 的 DemoLineReader）
        return new DemoLineReader(input, CS);
    }

    // ------------------------------------------------------------ 辅助

    private static void generateInputIfAbsent(File file, int lines) throws IOException {
        if (file.exists()) {
            return;
        }
        File dir = file.getParentFile();
        if (dir != null && !dir.exists() && !dir.mkdirs()) {
            throw new IOException("无法创建目录: " + dir);
        }
        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file, false), CS));
        try {
            for (int i = 1; i <= lines; i++) {
                writer.write(String.format("ORD-%06d|CUST-%05d|%.2f", i, i % 997, 9.9 + (i % 50)));
                writer.write("\n");
            }
        } finally {
            writer.close();
        }
        PipelineLog.info("已生成示例输入文件: %s（%d 行）", file.getAbsolutePath(), lines);
    }

    private static void preview(File file, String title, int maxLines) throws IOException {
        if (!file.exists()) {
            PipelineLog.warn("%s 不存在", file.getName());
            return;
        }
        List<String> lines = Files.readAllLines(file.toPath(), CS);
        System.out.println();
        System.out.println("----- " + title + " (共 " + lines.size() + " 行) -----");
        for (int i = 0; i < Math.min(maxLines, lines.size()); i++) {
            System.out.println(lines.get(i));
        }
        if (lines.size() > maxLines) {
            System.out.println("...");
        }
    }
}
