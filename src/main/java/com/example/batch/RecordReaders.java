package com.example.batch;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

/**
 * {@link RecordReader} 的便捷适配器与组合器：把现有的 Iterator / read() 形态读取工具接入模板，
 * 并覆盖"跳过表头、丢掉表尾"这类文件形态上的常见需求。
 *
 * <p>组合器都是装饰器——只改变读取行为，{@code close()} 一律委派给被包装的源 reader，
 * 因此可以按需叠加，例如：
 * <pre>
 * RecordReader&lt;String&gt; r = RecordReaders.footerAware(
 *         RecordReaders.headerAware(
 *                 RecordReaders.fromIterable(lines),
 *                 line -&gt; line.startsWith("#")),     // 跳过开头的注释行
 *         line -&gt; line.startsWith("TOTAL"));        // 丢掉结尾的汇总行
 * </pre>
 */
public final class RecordReaders {

    private RecordReaders() {
    }

    /** 允许抛 IO 异常的取值函数。 */
    public interface ExSupplier<T> {
        T get() throws IOException;
    }

    /** 允许抛 IO 异常的转换函数。 */
    public interface ExFunction<I, O> {
        O apply(I input) throws IOException;
    }

    // ------------------------------------------------------------------ 适配器

    /** 把 Iterator 适配为 RecordReader。 */
    public static <T> RecordReader<T> fromIterator(final Iterator<T> it) {
        return new IteratorReader<T>(it);
    }

    /** 把 Iterable 适配为 RecordReader。 */
    public static <T> RecordReader<T> fromIterable(final Iterable<T> iterable) {
        return fromIterator(iterable.iterator());
    }

    /**
     * 把 "read() 返回 null 表示结束" 形态的工具类适配为 RecordReader。
     * 内部通过预读缓存实现 hasNext()，原工具类无需改动。
     */
    public static <T> RecordReader<T> fromReadFunction(final ExSupplier<T> readFunc) {
        return new ReadFuncReader<T>(readFunc);
    }

    // ------------------------------------------------------------------ 组合器

    /** 丢弃开头的 {@code n} 条记录（如固定行数的文件头）。 */
    public static <T> RecordReader<T> skipping(RecordReader<T> source, long n) {
        return new SkippingReader<T>(source, n);
    }

    /** 只保留满足 {@code keep} 的记录（如去掉空行、注释行）。 */
    public static <T> RecordReader<T> filtering(RecordReader<T> source, Predicate<T> keep) {
        return new FilteringReader<T>(source, keep);
    }

    /**
     * 跳过<b>开头连续</b>匹配 {@code isHeader} 的记录（表头常见写法：首行是列名）。
     *
     * <p>只作用于开头：正文里再出现同形态的行仍会被正常处理。
     */
    public static <T> RecordReader<T> headerAware(RecordReader<T> source, Predicate<T> isHeader) {
        return new HeaderAwareReader<T>(source, isHeader);
    }

    /**
     * 丢弃<b>结尾连续</b>匹配 {@code isFooter} 的记录（表尾常见写法：末行是汇总/校验行）。
     *
     * <p>实现上需要预读一条：遇到表尾候选时先扣住，若后面还有记录，说明它不是真正的表尾，
     * 仍按原顺序交出；一路读到 EOF，则整批候选就是表尾，全部丢弃。
     * 正因如此，本组合器会预读，但缓存量只与"结尾连续表尾行数"同阶。
     */
    public static <T> RecordReader<T> footerAware(RecordReader<T> source, Predicate<T> isFooter) {
        return new FooterAwareReader<T>(source, isFooter);
    }

    /** 把源记录就地转换成另一种形态（如 String → 解析后的对象），转换允许抛 IO 异常。 */
    public static <I, O> RecordReader<O> mapping(RecordReader<I> source, ExFunction<I, O> mapper) {
        return new MappingReader<I, O>(source, mapper);
    }

    // ------------------------------------------------------------------ 实现

    private static final class IteratorReader<T> implements RecordReader<T> {
        private final Iterator<T> it;

        IteratorReader(Iterator<T> it) {
            this.it = it;
        }

        public boolean hasNext() {
            return it.hasNext();
        }

        public T next() {
            return it.next();
        }

        public void close() {
        }
    }

    private static final class ReadFuncReader<T> implements RecordReader<T> {
        private final ExSupplier<T> readFunc;
        private T pending;
        private boolean hasPending;
        private boolean exhausted;

        ReadFuncReader(ExSupplier<T> readFunc) {
            this.readFunc = readFunc;
        }

        public boolean hasNext() throws IOException {
            ensurePending();
            return hasPending;
        }

        public T next() throws IOException {
            ensurePending();
            if (!hasPending) {
                throw new NoSuchElementException("reader already exhausted");
            }
            T value = pending;
            pending = null;
            hasPending = false;
            return value;
        }

        /** 预读一条缓存起来，用于支撑 hasNext()。 */
        private void ensurePending() throws IOException {
            if (hasPending || exhausted) {
                return;
            }
            T value = readFunc.get();
            if (value == null) {
                exhausted = true;
            } else {
                pending = value;
                hasPending = true;
            }
        }

        public void close() {
            pending = null;
            hasPending = false;
            exhausted = true;
        }
    }

    private static final class SkippingReader<T> implements RecordReader<T> {
        private final RecordReader<T> source;
        private long remaining;
        private boolean started;

        SkippingReader(RecordReader<T> source, long n) {
            this.source = source;
            this.remaining = n;
        }

        public boolean hasNext() throws IOException {
            skipLeading();
            return source.hasNext();
        }

        public T next() throws IOException {
            skipLeading();
            return source.next();
        }

        public void close() throws IOException {
            source.close();
        }

        /** 只在首次访问时丢弃开头若干条。 */
        private void skipLeading() throws IOException {
            if (started) {
                return;
            }
            started = true;
            while (remaining > 0 && source.hasNext()) {
                source.next();
                remaining--;
            }
        }
    }

    private static final class FilteringReader<T> implements RecordReader<T> {
        private final RecordReader<T> source;
        private final Predicate<T> keep;
        private T pending;
        private boolean hasPending;
        private boolean sourceExhausted;

        FilteringReader(RecordReader<T> source, Predicate<T> keep) {
            this.source = source;
            this.keep = keep;
        }

        public boolean hasNext() throws IOException {
            advance();
            return hasPending;
        }

        public T next() throws IOException {
            advance();
            if (!hasPending) {
                throw new NoSuchElementException("reader already exhausted");
            }
            T value = pending;
            pending = null;
            hasPending = false;
            return value;
        }

        public void close() throws IOException {
            source.close();
        }

        /** 往后读到第一条满足 keep 的记录并扣住，用于支撑 hasNext()。 */
        private void advance() throws IOException {
            while (!hasPending && !sourceExhausted) {
                if (!source.hasNext()) {
                    sourceExhausted = true;
                    return;
                }
                T value = source.next();
                if (value != null && keep.test(value)) {
                    pending = value;
                    hasPending = true;
                }
            }
        }
    }

    private static final class HeaderAwareReader<T> implements RecordReader<T> {
        private final RecordReader<T> source;
        private final Predicate<T> isHeader;
        private T pending;
        private boolean hasPending;
        private boolean started;

        HeaderAwareReader(RecordReader<T> source, Predicate<T> isHeader) {
            this.source = source;
            this.isHeader = isHeader;
        }

        public boolean hasNext() throws IOException {
            skipHeader();
            return hasPending || source.hasNext();
        }

        public T next() throws IOException {
            skipHeader();
            if (hasPending) {
                T value = pending;
                pending = null;
                hasPending = false;
                return value;
            }
            return source.next();
        }

        public void close() throws IOException {
            source.close();
        }

        /** 首次访问时丢掉开头连续的表头行，第一条正文行扣在 pending 里。 */
        private void skipHeader() throws IOException {
            if (started) {
                return;
            }
            started = true;
            while (source.hasNext()) {
                T value = source.next();
                if (value != null && isHeader.test(value)) {
                    continue;   // 仍是表头，丢弃
                }
                if (value != null) {
                    pending = value;
                    hasPending = true;
                }
                return;
            }
        }
    }

    private static final class FooterAwareReader<T> implements RecordReader<T> {
        private final RecordReader<T> source;
        private final Predicate<T> isFooter;
        private final Deque<T> buffer = new ArrayDeque<T>();
        private boolean sourceExhausted;

        FooterAwareReader(RecordReader<T> source, Predicate<T> isFooter) {
            this.source = source;
            this.isFooter = isFooter;
        }

        public boolean hasNext() throws IOException {
            fill();
            return !buffer.isEmpty();
        }

        public T next() throws IOException {
            fill();
            if (buffer.isEmpty()) {
                throw new NoSuchElementException("reader already exhausted");
            }
            return buffer.removeFirst();
        }

        public void close() throws IOException {
            source.close();
        }

        /**
         * 预读到「一条非表尾记录」或 EOF 为止。
         *
         * <p>中途遇到的表尾候选先扣在 buffer 里：只要后面还有记录，它们就不是真正的表尾，
         * 会按原顺序交出；只有一路读到 EOF 时，buffer 尾部连续匹配 isFooter 的那些才是表尾。
         */
        private void fill() throws IOException {
            while (!sourceExhausted) {
                if (!source.hasNext()) {
                    sourceExhausted = true;
                    dropTrailingFooters();
                    return;
                }
                T value = source.next();
                if (value == null) {
                    continue;
                }
                buffer.addLast(value);
                if (!isFooter.test(value)) {
                    return;           // 遇到正文行：buffer 里的内容全部有效
                }
                // 是表尾候选：继续往后看
            }
        }

        /** EOF 时把 buffer 尾部连续匹配 isFooter 的记录丢弃——它们正是文件表尾。 */
        private void dropTrailingFooters() {
            while (!buffer.isEmpty() && isFooter.test(buffer.peekLast())) {
                buffer.removeLast();
            }
        }
    }

    private static final class MappingReader<I, O> implements RecordReader<O> {
        private final RecordReader<I> source;
        private final ExFunction<I, O> mapper;

        MappingReader(RecordReader<I> source, ExFunction<I, O> mapper) {
            this.source = source;
            this.mapper = mapper;
        }

        public boolean hasNext() throws IOException {
            return source.hasNext();
        }

        public O next() throws IOException {
            return mapper.apply(source.next());
        }

        public void close() throws IOException {
            source.close();
        }
    }
}
