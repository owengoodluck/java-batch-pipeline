package com.example.batch;

import java.io.Closeable;

/**
 * 记录读取器（SPI：每种文件类型提供自己的实现）。
 *
 * <p>采用 hasNext()/next() 形态而非 read() 返回 null，原因：
 * <ol>
 *   <li>结束语义明确，不依赖"null 表示读完"的隐含约定；</li>
 *   <li>允许抛受检异常，IO 异常可以原样穿透给模板处理（JDK 的 Iterator 做不到）。</li>
 * </ol>
 *
 * <p>如果手上已有 Iterator 或 read() 形态的工具类，用 {@link RecordReaders} 一行适配即可。
 */
public interface RecordReader<T> extends Closeable {

    /** 是否还有下一条记录。 */
    boolean hasNext() throws Exception;

    /** 返回下一条记录（仅在 hasNext() 为 true 后调用）。 */
    T next() throws Exception;
}
