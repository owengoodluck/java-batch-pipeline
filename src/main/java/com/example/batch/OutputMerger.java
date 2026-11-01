package com.example.batch;

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
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 合并器：把一个通道分片目录下的 {@code *.part} 按批次序号升序，流式追加成一个最终文件。
 *
 * <p>先写临时文件再 rename，保证下游永远不会读到写了一半的输出文件。
 *
 * <p>多通道下每个通道独立合并，互不影响；某个通道合并失败不会破坏其他通道已生成的文件。
 */
public final class OutputMerger {

    private static final String PART_SUFFIX = ".part";

    private OutputMerger() {
    }

    /**
     * @param channelPartsDir 单个通道的分片目录（{@code parts/{key}}），不存在视为无数据
     * @param target          该通道的最终输出文件
     * @param createEmptyOutput 无分片时是否仍生成空文件
     * @return 合并出的行数
     */
    public static long merge(File channelPartsDir, File target, Charset charset,
                             boolean createEmptyOutput) throws IOException {
        List<File> parts = listParts(channelPartsDir);
        if (parts.isEmpty() && !createEmptyOutput) {
            return 0L;
        }
        ensureParentDir(target);
        File tmp = new File(target.getParentFile(), target.getName() + ".tmp");

        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(tmp, false), charset));
        long lines = 0L;
        try {
            for (File part : parts) {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(new FileInputStream(part), charset));
                try {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        writer.write(line);
                        writer.write("\n");
                        lines++;
                    }
                } finally {
                    reader.close();
                }
            }
        } finally {
            writer.close();
        }
        moveAtomically(tmp, target);
        return lines;
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

    private static void ensureParentDir(File target) throws IOException {
        File parent = target.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
            throw new IOException("无法创建输出目录: " + parent);
        }
    }

    private static void moveAtomically(File from, File to) throws IOException {
        try {
            Files.move(from.toPath(), to.toPath(),
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            // 跨文件系统等场景下 ATOMIC_MOVE 不支持，退化为普通移动
            Files.move(from.toPath(), to.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
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
