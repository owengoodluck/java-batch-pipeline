package com.example.batch;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * {@link RecordReader} 的便捷适配器，用于把现有的 Iterator / read() 形态读取工具接入模板。
 */
public final class RecordReaders {

    private RecordReaders() {
    }

    /** 允许抛受检异常的取值函数。 */
    public interface ExSupplier<T> {
        T get() throws Exception;
    }

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

        public boolean hasNext() throws Exception {
            ensurePending();
            return hasPending;
        }

        public T next() throws Exception {
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
        private void ensurePending() throws Exception {
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
}
