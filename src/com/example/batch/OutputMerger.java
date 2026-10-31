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
 * 合并器：把分片目录里的 *.{ext}.part 按批次序号升序，流式追加成一个最终文件。
 *
 * <p>先写临时文件再 rename，保证下游永远不会读到写了一半的输出文件。
 */
public final class OutputMerger {

    private OutputMerger() {
    }

    /**
     * @param partsDir 分片目录
     * @param target   最终输出文件
     * @param ext      PartWriter.SUCCESS_EXT 或 PartWriter.ERROR_EXT
     * @return 合并出的行数
     */
    public static long merge(File partsDir, File target, String ext, Charset charset,
                             boolean createEmptyOutput) throws IOException {
        List<File> parts = listParts(partsDir, ext);
        long lines = 0;
        if (parts.isEmpty() && !createEmptyOutput) {
            return 0;
        }
        ensureParentDir(target);
        File tmp = new File(target.getParentFile(), target.getName() + ".tmp");

        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(tmp, false), charset));
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

    /** 分片文件名带零填充序号，字典序即批次顺序，因此天然保持文件行序。 */
    private static List<File> listParts(File partsDir, String ext) {
        String suffix = "." + ext + ".part";
        File[] files = partsDir.listFiles();
        List<File> result = new ArrayList<File>();
        if (files != null) {
            for (File f : files) {
                if (f.isFile() && f.getName().endsWith(suffix)) {
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

    /** 清理分片目录（仅在合并成功后调用）。 */
    public static void cleanup(File partsDir) throws IOException {
        if (!partsDir.exists()) {
            return;
        }
        File[] files = partsDir.listFiles();
        if (files != null) {
            for (File f : files) {
                f.delete();
            }
        }
        partsDir.delete();
    }
}
