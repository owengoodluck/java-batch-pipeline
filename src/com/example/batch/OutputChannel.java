package com.example.batch;

import java.io.File;
import java.util.regex.Pattern;

/**
 * 一个输出通道：把"一类结果"绑定到"一个目标文件 + 一套行格式"。
 *
 * <p>通道是 {@link PipelineConfig} 声明期的一等公民，但运行时只认 key：
 * 分片目录名、合并目标、日志标签全部由 key 驱动，与业务含义无关。
 *
 * <p><b>key 必须是稳定的 ASCII 标识</b>（如 {@code paid} / {@code shipped} / {@code error}），
 * 不要直接用中文状态名。原因很实际：key 会被用作分片子目录名
 * （{@code parts/paid/part-000001.part}），中文目录名在 macOS/Linux 上没问题，
 * 一旦跨到 Windows 打包、zip 解压、CI 缓存就经常乱码。业务文案放 formatter 里。
 *
 * <p><b>未声明的 key 不会动态建通道</b>：路由返回了未注册的 key，一律落 {@link #ERROR}
 * 通道并计数告警。这是刻意的——否则一个拼写错误（{@code "paid "} 多了个空格）
 * 就会静默多出一组文件，下游读到旧文件也毫无察觉，等对账日才暴露。
 *
 * @param <T> 原始记录类型
 * @param <P> 附加内容类型
 */
public final class OutputChannel<T, P> {

    /** 成功通道的保留 key：未实现 {@link RoutingProcessor} 时的默认目标。 */
    public static final String SUCCESS = "success";

    /** 错误通道的保留 key：记录级失败、批次级失败、未注册路由全部落这里，必须存在。 */
    public static final String ERROR = "error";

    /** 允许字母数字开头，仅含字母数字与 . _ -，最长 48：既要能当目录名，也要能进 URL 与配置。 */
    private static final Pattern KEY_PATTERN = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,47}");

    private final String key;
    private final File target;
    private final LineFormatter<T, P> formatter;
    private final boolean createEmpty;
    private final boolean errorChannel;

    private OutputChannel(String key, File target, LineFormatter<T, P> formatter,
                          boolean createEmpty, boolean errorChannel) {
        if (key == null || !KEY_PATTERN.matcher(key).matches()) {
            throw new IllegalArgumentException(
                    "channel key 非法（字母/数字开头，仅含字母数字 . _ -，长度 1..48）: " + key);
        }
        if (target == null) {
            throw new IllegalArgumentException("channel [" + key + "] 的 target 文件不能为空");
        }
        if (formatter == null) {
            throw new IllegalArgumentException("channel [" + key + "] 的 formatter 不能为空");
        }
        this.key = key;
        this.target = target;
        this.formatter = formatter;
        this.createEmpty = createEmpty;
        this.errorChannel = errorChannel;
    }

    /** 普通业务通道。 */
    public static <T, P> OutputChannel<T, P> of(String key, File target, LineFormatter<T, P> formatter) {
        return new OutputChannel<T, P>(key, target, formatter, false, false);
    }

    /** 错误通道：承载记录级失败、批次级失败与未注册路由。 */
    public static <T, P> OutputChannel<T, P> error(File target, LineFormatter<T, P> formatter) {
        return new OutputChannel<T, P>(ERROR, target, formatter, false, true);
    }

    /**
     * 该通道本次运行一条记录都没有时，是否仍生成一个空文件。
     *
     * <p>默认 false——分片机制本身就是统计：没有记录就没有分片，合并阶段扫目录即知，
     * 不需要额外计数。只有下游"必须拿到固定几个文件"时才打开。
     */
    public OutputChannel<T, P> createEmpty(boolean value) {
        return new OutputChannel<T, P>(key, target, formatter, value, errorChannel);
    }

    public String getKey() {
        return key;
    }

    public File getTarget() {
        return target;
    }

    public LineFormatter<T, P> getFormatter() {
        return formatter;
    }

    public boolean isCreateEmpty() {
        return createEmpty;
    }

    public boolean isErrorChannel() {
        return errorChannel;
    }

    /** 该通道的分片子目录：{@code parts/{key}}。 */
    public File partsDirOf(File rootPartsDir) {
        return new File(rootPartsDir, key);
    }

    @Override
    public String toString() {
        return key + " -> " + target.getName();
    }
}
