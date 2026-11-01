package com.example.batch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 合并器：把一个通道分片目录下的 {@code *.part} 按批次序号升序，流式写入该通道的输出目标并提交。
 *
 * <p>写入与提交分离（{@link ChannelTarget#open()} / {@link ChannelTarget#commit()}），
 * 保证下游永远不会读到写了一半的输出：只有 commit 成功之后结果才对外可见。
 *
 * <p>多通道下每个通道独立合并，互不影响；某个通道合并失败不会破坏其他通道已生成的结果。
 */
public final class OutputMerger {

    private static final String PART_SUFFIX = ".part";

    private OutputMerger() {
    }

    /**
     * @param channelPartsDir 单个通道的分片目录（{@code parts/{key}}），不存在视为无数据
     * @param target          该通道的输出目标（落文件 / 上传远端等）
     * @param createEmptyOutput 无数据时是否仍产出空结果
     * @return 合并出的行数
     */
    public static long merge(File channelPartsDir, ChannelTarget target, Charset charset,
                             boolean createEmptyOutput) throws IOException {
        List<File> parts = listParts(channelPartsDir);
        if (parts.isEmpty() && !createEmptyOutput) {
            return 0L;
        }
        OutputStream out = target.open();
        long lines = 0L;
        try {
            Writer writer = new BufferedWriter(new OutputStreamWriter(out, charset));
            for (File part : parts) {
                lines += appendPart(part, writer, charset);
            }
            writer.flush();
        } finally {
            out.close();
        }
        target.commit();
        return lines;
    }

    /** 把一个分片逐行追加到输出，返回追加的行数。 */
    private static long appendPart(File part, Writer writer, Charset charset) throws IOException {
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(part), charset));
        try {
            long lines = 0L;
            String line;
            while ((line = reader.readLine()) != null) {
                writer.write(line);
                writer.write("\n");
                lines++;
            }
            return lines;
        } finally {
            reader.close();
        }
    }

    /** 分片文件名带零填充序号，字典序即批次顺序，因此通道内部天然保持输入行序。 */
    private static List<File> listParts(File channelPartsDir) {
        List<File> result = new ArrayList<File>();
        if (channelPartsDir == null || !channelPartsDir.isDirectory()) {
            return result;
        }
        File[] files = channelPartsDir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isFile() && f.getName().endsWith(PART_SUFFIX)) {
                    result.add(f);
                }
            }
        }
        Collections.sort(result);
        return result;
    }

    /** 清理分片目录（仅在全部通道合并成功后调用）。多通道下需要递归删除各通道路径。 */
    public static void cleanup(File partsDir) throws IOException {
        if (partsDir == null || !partsDir.exists()) {
            return;
        }
        deleteChildren(partsDir);
        partsDir.delete();
    }

    static void deleteChildren(File dir) {
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }
        for (File f : files) {
            if (f.isDirectory()) {
                deleteChildren(f);
            }
            if (!f.delete()) {
                PipelineLog.warn("清理分片失败（可能被占用）: %s", f.getAbsolutePath());
            }
        }
    }
}
