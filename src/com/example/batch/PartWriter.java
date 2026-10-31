package com.example.batch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 分片写入器：每个批次按通道路由，把结果写进**各通道自己的子目录**。
 *
 * <p>分片布局（根目录是 {@code partsDir}）：
 * <pre>
 * parts/
 * ├── paid/part-000001.part
 * ├── shipped/part-000001.part
 * └── error/part-000002.part
 * </pre>
 *
 * <p>按通道分目录而不是靠文件名后缀（{@code part-000001.paid.part}），有三个实际好处：
 * 合并时扫一个目录就够、通道 key 不必参与文件名字符集处理、清理/排查时能直接按通道定位。
 *
 * <p>并行阶段每个批次只写自己的分片文件，同一通道的不同批次写的是不同文件，
 * 因此仍然完全无锁；最后统一由 {@link OutputMerger} 按批次序号合并。
 */
public final class PartWriter<T, P> {

    private static final String PART_SUFFIX = ".part";

    private final File partsDir;
    private final Charset charset;
    private final PipelineConfig<T, P> config;
    private final ChannelRouter<T, P> router;

    public PartWriter(File partsDir, Charset charset, PipelineConfig<T, P> config,
                      ChannelRouter<T, P> router) {
        this.partsDir = partsDir;
        this.charset = charset;
        this.config = config;
        this.router = router;
    }

    /**
     * 写入一批的处理结果，按通道路由到各自分片。
     *
     * <p>只有真正有记录的通道才会被打开并落盘，因此「本批该通道为空」不会留下 0 字节分片，
     * 合并阶段自然就知道哪些通道本次无数据。
     *
     * @return 各通道本批写入的行数（按通道声明顺序），没有记录的通道不会出现在结果里
     */
    public Map<String, Integer> writeResults(long batchSeq, List<T> batch,
                                             List<ItemResult<P>> results) throws IOException {
        Map<String, Integer> counts = new LinkedHashMap<String, Integer>();
        Map<String, BufferedWriter> writers = new HashMap<String, BufferedWriter>();
        try {
            for (int i = 0; i < batch.size(); i++) {
                T record = batch.get(i);
                ItemResult<P> result = results.get(i);
                ChannelRouter.Route route = router.resolve(i, record, result);
                String key = route.getKey();
                OutputChannel<T, P> channel = config.getChannel(key);
                if (channel == null) {
                    // 路由层保证返回已注册 key，走到这里说明配置在运行期被改坏了
                    throw new IOException("通道路由失败，未找到已注册通道: " + key);
                }

                BufferedWriter writer = writers.get(key);
                if (writer == null) {
                    writer = openWriter(partFile(key, batchSeq));
                    writers.put(key, writer);
                }
                writeLine(writer, format(channel, record, result, route));

                Integer prev = counts.get(key);
                counts.put(key, prev == null ? 1 : prev + 1);
            }
        } finally {
            for (BufferedWriter w : writers.values()) {
                closeQuietly(w);
            }
        }
        return counts;
    }

    /** 整批调用失败：全部记录写入错误通道的分片（级别 BATCH）。 */
    public int writeBatchFailed(long batchSeq, List<T> batch, String errorCode, String message)
            throws IOException {
        String key = config.getErrorChannelKey();
        OutputChannel<T, P> channel = config.getChannel(key);
        File part = partFile(key, batchSeq);
        BufferedWriter writer = openWriter(part);
        int n = 0;
        try {
            for (T record : batch) {
                writeLine(writer, channel.getFormatter()
                        .formatBatchFailed(record, errorCode, message));
                n++;
            }
        } finally {
            closeQuietly(writer);
        }
        return n;
    }

    /**
     * 选择行格式。
     *
     * <p>兜底记录（请求了未注册通道）即便业务上是成功的，也必须走**错误格式**：
     * 否则错误文件里会混入列数不同的行，下游按列解析直接错位。
     */
    private String format(OutputChannel<T, P> channel, T record, ItemResult<P> result,
                          ChannelRouter.Route route) {
        if (route.isFallback()) {
            ItemResult<P> synthetic = ItemResult.failed(
                    "UNROUTED:" + route.getRequestedKey(),
                    "通道 " + route.getRequestedKey() + " 未注册（原始结果码 " + result.getCode() + "）");
            return channel.getFormatter().formatRecordFailed(record, synthetic);
        }
        if (result.isSuccess()) {
            return channel.getFormatter().formatSuccess(record, result);
        }
        return channel.getFormatter().formatRecordFailed(record, result);
    }

    private File partFile(String channelKey, long batchSeq) throws IOException {
        File dir = new File(partsDir, channelKey);
        if (!dir.isDirectory()) {
            // 多个消费者线程可能同时首次进入同一个通道，mkdirs 失败但目录已存在属于正常竞争
            if (!dir.mkdirs() && !dir.isDirectory()) {
                throw new IOException("无法创建通道分片目录: " + dir.getAbsolutePath());
            }
        }
        return new File(dir, partName(batchSeq) + PART_SUFFIX);
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
                // 关闭失败不影响主流程：数据已 flush 到内核缓冲
            }
        }
    }

    /** 列出所有通道的分片根目录（用于清理与排查）。 */
    static List<File> allChannelDirs(File rootPartsDir) {
        List<File> dirs = new ArrayList<File>();
        File[] children = rootPartsDir.listFiles();
        if (children != null) {
            for (File f : children) {
                if (f.isDirectory()) {
                    dirs.add(f);
                }
            }
        }
        return dirs;
    }
}
