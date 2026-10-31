package com.example.batch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.List;

/**
 * 分片写入器：每个批次按结果分流写入本批次自己的两个分片文件。
 *
 * <p>并行阶段各线程只写自己的文件，因此不需要任何锁；最后统一由 {@link OutputMerger} 合并。
 */
public final class PartWriter<T, P> {

    /** 成功分片后缀 */
    public static final String SUCCESS_EXT = "success";
    /** 错误分片后缀 */
    public static final String ERROR_EXT = "error";

    private final File partsDir;
    private final Charset charset;
    private final PipelineConfig<T, P> config;

    public PartWriter(File partsDir, Charset charset, PipelineConfig<T, P> config) {
        this.partsDir = partsDir;
        this.charset = charset;
        this.config = config;
    }

    /**
     * 写入一批的处理结果：成功行进 success 分片，记录级失败行进 error 分片。
     *
     * @return 长度为 2 的数组：[成功行数, 失败行数]
     */
    public int[] writeResults(long batchSeq, List<T> batch, List<ItemResult<P>> results)
            throws IOException {
        int ok = 0;
        int ng = 0;
        String base = partName(batchSeq);
        File successPart = new File(partsDir, base + "." + SUCCESS_EXT + ".part");
        File errorPart = new File(partsDir, base + "." + ERROR_EXT + ".part");

        BufferedWriter successWriter = null;
        BufferedWriter errorWriter = null;
        try {
            for (int i = 0; i < batch.size(); i++) {
                T record = batch.get(i);
                ItemResult<P> result = results.get(i);
                if (result.isSuccess()) {
                    if (successWriter == null) {
                        successWriter = openWriter(successPart);
                    }
                    writeLine(successWriter, config.getSuccessFormatter().formatSuccess(record, result));
                    ok++;
                } else {
                    if (errorWriter == null) {
                        errorWriter = openWriter(errorPart);
                    }
                    writeLine(errorWriter,
                            config.getErrorFormatter().formatRecordFailed(record, result));
                    ng++;
                }
            }
        } finally {
            closeQuietly(successWriter);
            closeQuietly(errorWriter);
        }
        // 空文件不留痕，避免合并阶段扫到一堆 0 字节文件
        if (ok == 0) {
            deleteQuietly(successPart);
        }
        if (ng == 0) {
            deleteQuietly(errorPart);
        }
        return new int[] {ok, ng};
    }

    /** 整批调用失败：全部记录写入 error 分片（级别 BATCH）。 */
    public int writeBatchFailed(long batchSeq, List<T> batch, String errorCode, String message)
            throws IOException {
        File errorPart = new File(partsDir, partName(batchSeq) + "." + ERROR_EXT + ".part");
        BufferedWriter writer = openWriter(errorPart);
        int n = 0;
        try {
            for (T record : batch) {
                writeLine(writer, config.getErrorFormatter()
                        .formatBatchFailed(record, errorCode, message));
                n++;
            }
        } finally {
            closeQuietly(writer);
        }
        return n;
    }

    public static String partName(long batchSeq) {
        return String.format("part-%06d", batchSeq);
    }

    private BufferedWriter openWriter(File file) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, false), charset));
    }

    private void writeLine(Writer writer, String line) throws IOException {
        writer.write(line);
        writer.write("\n");
    }

    private void closeQuietly(Writer writer) {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException ignored) {
                // 关闭失败不影响主流程
            }
        }
    }

    private void deleteQuietly(File file) {
        if (file.exists()) {
            file.delete();
        }
    }
}
