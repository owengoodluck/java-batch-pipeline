package com.example.demo;

import com.example.batch.BatchPipelines;
import com.example.batch.BatchResult;
import com.example.batch.ChannelTargets;
import com.example.batch.PipelineConfig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * 可运行 Demo：演示「批量前按字段分组」的<b>内置分组批处理</b>（最新设计，无需外部全量读入）。
 *
 * <p>输入文件里各组记录是交错混排的（G1/G2/G3 交错）。本 Demo 通过
 * {@code .groupBy(r -> r.getKey())} 启用内置分组：框架每读一条就按 key 入桶，
 * 某桶攒满 batchSize 即作为一个「单组批次」提交下游——整个过程在<b>一条流水线内</b>完成，
 * 不在内存里一次性缓存全部记录。
 *
 * <p>运行：java com.example.demo.DemoGroupMain [工作目录]
 */
public class DemoGroupMain {

    private static final Charset CS = StandardCharsets.UTF_8;
    private static final int TOTAL_LINES = 120;          // 3 组 × 40 条
    private static final int BATCH_SIZE = 10;            // 每组 40 条 → 每组切成 4 个「单组批次」
    private static final String[] GROUPS = {"G1", "G2", "G3"};

    public static void main(String[] args) throws Exception {
        File base = new File(args.length > 0 ? args[0] : "demo-run/group");
        File input = new File(base, "input/sample.txt");
        File outDir = new File(base, "output");
        File partsDir = new File(base, "parts");

        generateInputIfAbsent(input, TOTAL_LINES);

        // 一条流水线 + 内置分组：按 key 字段分组，每攒满 BATCH_SIZE 个同组记录才真正调一次下游。
        BatchResult result = BatchPipelines.run(
                PipelineConfig.<DemoGroupRecord, String>builder()
                        .reader(new DemoGroupReader(input, CS))
                        .processor(new DemoGroupProcessor())
                        .groupBy(r -> r.getKey())                 // ← 内置分组：按 key 字段
                        .batchSize(BATCH_SIZE)
                        .consumerThreads(2)
                        .bufferBatches(2)
                        .charset(CS)
                        .partsDir(partsDir)
                        .channel("success", ChannelTargets.file(new File(outDir, "group.success")),
                                DemoGroupFormatter.SUCCESS, true)
                        .errorChannel(ChannelTargets.file(new File(outDir, "group.error")),
                                DemoGroupFormatter.ERROR, true)
                        .build());

        System.out.println();
        System.out.println(result);
        System.out.println();
        System.out.println("输出文件: " + outDir + "/group.success 与 group.error");
        System.out.println("（每条成功行形如 'Gx\\tPAYLOAD-n'；processor 日志会打印每个单组批次的组号，");
        System.out.println("  可看到同一次下游调用始终只含一个组，证明「单组保证」。）");
    }

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
                // 交错写入 G1/G2/G3，模拟「原始文件里各组记录是混在一起的」
                String key = GROUPS[(i - 1) % GROUPS.length];
                writer.write(key + "\tPAYLOAD-" + i);
                writer.write("\n");
            }
        } finally {
            writer.close();
        }
        System.out.println("已生成示例输入文件: " + file.getAbsolutePath() + "（" + lines + " 行，"
                + GROUPS.length + " 组交错）");
    }
}
