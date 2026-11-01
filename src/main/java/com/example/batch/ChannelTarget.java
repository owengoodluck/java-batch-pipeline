package com.example.batch;

import java.io.IOException;
import java.io.OutputStream;

/**
 * 一个输出通道的<b>最终归宿</b>：框架把该通道合并后的完整结果作为字节流交给你，由你决定写到哪里。
 *
 * <p>契约是<b>两阶段</b>的，对应流水线自身的并行阶段与收尾阶段：
 * <ol>
 *   <li><b>写入</b> {@link #open()}：返回一个暂存输出流，框架把该通道全部分片按批次序号
 *       依次写入，写完即关闭；</li>
 *   <li><b>提交</b> {@link #commit()}：流关闭之后调用，此时才让结果对外可见
 *       （本地文件走 rename，对象存储走上传）。</li>
 * </ol>
 *
 * <p>拆成两步是为了守住框架的核心保证：<b>下游永远不会看到写了一半的输出</b>。
 * 写入期间结果处于暂存态，只有 {@code commit} 成功之后才对外可见；
 * {@code commit} 失败则整条流水线按失败处理，分片保留以便重试或排查。
 *
 * <p>落本地文件用 {@link ChannelTargets#file(File)}；其它介质（S3/OSS、HTTP 上传、
 * 标准输出等）实现本接口传入即可——因此框架本身不需要任何第三方 SDK。
 *
 * <p>框架保证：同一通道的 {@code open → close → commit} 严格串行且只发生一次；
 * 不同通道之间互不干扰。实现无需考虑并发。
 *
 * <p>建议覆写 {@code toString()} 返回一个可读的目标标识（如文件名、{@code bucket/key}），
 * 它会出现在"通道 X → 目标 Y 共 N 行"的合并日志里。
 */
public interface ChannelTarget {

    /**
     * 打开暂存输出流。
     *
     * <p>实现负责让这个流「可写」（例如本地文件场景需要确保父目录存在），
     * 并负责返回的流在写入期间不对外暴露结果。
     *
     * @return 暂存输出流，由框架负责关闭
     */
    OutputStream open() throws IOException;

    /**
     * 提交：{@link #open()} 返回的流已被关闭之后调用，此时才让结果对外可见。
     *
     * <p>只有在确实有内容需要产出时才会被调用——通道无数据时，
     * 除非声明了 {@code createEmpty}，否则框架不会走 open/commit。
     */
    void commit() throws IOException;
}
