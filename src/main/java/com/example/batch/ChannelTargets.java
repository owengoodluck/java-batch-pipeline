package com.example.batch;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * {@link ChannelTarget} 的工厂方法集合。
 *
 * <p>框架只内置落本地文件这一种目标；其它介质由使用方实现 {@link ChannelTarget} 传入，
 * 因此本框架保持零第三方依赖。
 *
 * <pre>
 * .channel("success", ChannelTargets.file(new File(dir, "order.success")), fmt, true)
 * </pre>
 */
public final class ChannelTargets {

    private static final String STAGING_SUFFIX = ".tmp";

    private ChannelTargets() {
    }

    /**
     * 落本地文件：先写同目录下的 {@code .tmp} 暂存文件，提交时原子 rename 成目标文件。
     *
     * <p>暂存文件与目标文件放在同一目录，是为了保证 rename 落在同一文件系统上，
     * 从而拿到文件系统级别的原子性。
     */
    public static ChannelTarget file(File file) {
        if (file == null) {
            throw new IllegalArgumentException("目标文件不能为空");
        }
        return new FileChannelTarget(file);
    }

    /** 落本地文件的目标实现（包内可见：使用方请走 {@link ChannelTargets#file(File)}）。 */
    static final class FileChannelTarget implements ChannelTarget {

        private final File file;
        private File staging;

        FileChannelTarget(File file) {
            this.file = file;
        }

        public OutputStream open() throws IOException {
            File parent = file.getParentFile();
            if (parent != null && !parent.exists() && !parent.mkdirs()) {
                throw new IOException("无法创建输出目录: " + parent);
            }
            staging = new File(parent, file.getName() + STAGING_SUFFIX);
            return new BufferedOutputStream(new FileOutputStream(staging, false));
        }

        public void commit() throws IOException {
            try {
                Files.move(staging.toPath(), file.toPath(),
                        StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                // 跨文件系统等场景下 ATOMIC_MOVE 不支持，退化为普通移动
                Files.move(staging.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }

        @Override
        public String toString() {
            return file.getName();
        }
    }
}
