package com.example.batch;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * 读取层组合器测试：覆盖「跳过表头」「丢掉表尾」「过滤」「映射」四类常见文件形态需求，
 * 并验证组合器作为装饰器会把 close() 委派给源 reader。
 */
public class RecordReadersTest {

    private static <T> List<T> drain(RecordReader<T> reader) throws IOException {
        List<T> out = new ArrayList<T>();
        while (reader.hasNext()) {
            out.add(reader.next());
        }
        return out;
    }

    @Test
    public void skippingDropsLeadingRecords() throws IOException {
        RecordReader<String> r = RecordReaders.skipping(
                RecordReaders.fromIterable(Arrays.asList("h1", "h2", "a", "b")), 2);
        assertEquals(Arrays.asList("a", "b"), drain(r));
    }

    @Test
    public void skippingMoreThanAvailableYieldsEmpty() throws IOException {
        RecordReader<String> r = RecordReaders.skipping(
                RecordReaders.fromIterable(Arrays.asList("a")), 5);
        assertTrue(drain(r).isEmpty());
    }

    @Test
    public void filteringKeepsOnlyMatchingRecords() throws IOException {
        Predicate<String> notBlank = s -> !s.trim().isEmpty();
        RecordReader<String> r = RecordReaders.filtering(
                RecordReaders.fromIterable(Arrays.asList("a", "", "b", "  ", "c")), notBlank);
        assertEquals(Arrays.asList("a", "b", "c"), drain(r));
    }

    @Test
    public void mappingTransformsRecords() throws IOException {
        RecordReader<Integer> r = RecordReaders.mapping(
                RecordReaders.fromIterable(Arrays.asList("1", "2", "3")), Integer::valueOf);
        assertEquals(Arrays.asList(1, 2, 3), drain(r));
    }

    @Test
    public void headerAwareSkipsOnlyLeadingHeaderLines() throws IOException {
        Predicate<String> isHeader = s -> s.startsWith("#");
        RecordReader<String> r = RecordReaders.headerAware(
                RecordReaders.fromIterable(Arrays.asList("#c1", "#c2", "r1", "#note", "r2")),
                isHeader);
        // 开头两条表头被跳过；正文里同形态的行不受影响
        assertEquals(Arrays.asList("r1", "#note", "r2"), drain(r));
    }

    @Test
    public void footerAwareDropsOnlyTrailingFooterLines() throws IOException {
        Predicate<String> isFooter = s -> s.startsWith("TOTAL") || "END".equals(s);
        RecordReader<String> r = RecordReaders.footerAware(
                RecordReaders.fromIterable(Arrays.asList("r1", "TOTAL 1", "r2", "TOTAL 2", "END")),
                isFooter);
        // 结尾连续的 TOTAL 2 / END 是表尾 → 丢弃；正文中间的 TOTAL 1 保留
        assertEquals(Arrays.asList("r1", "TOTAL 1", "r2"), drain(r));
    }

    @Test
    public void footerAwareKeepsAllWhenNoFooterPresent() throws IOException {
        Predicate<String> isFooter = s -> s.startsWith("TOTAL");
        RecordReader<String> r = RecordReaders.footerAware(
                RecordReaders.fromIterable(Arrays.asList("r1", "r2", "r3")), isFooter);
        assertEquals(Arrays.asList("r1", "r2", "r3"), drain(r));
    }

    @Test
    public void footerAwareOnAllFooterInputYieldsEmpty() throws IOException {
        Predicate<String> isFooter = s -> s.startsWith("TOTAL");
        RecordReader<String> r = RecordReaders.footerAware(
                RecordReaders.fromIterable(Arrays.asList("TOTAL 1", "TOTAL 2")), isFooter);
        assertTrue(drain(r).isEmpty());
    }

    @Test
    public void combinatorsComposeAndDelegateClose() throws IOException {
        final boolean[] closed = {false};
        RecordReader<String> src = new RecordReader<String>() {
            public boolean hasNext() {
                return false;
            }

            public String next() {
                throw new NoSuchElementException("empty");
            }

            public void close() {
                closed[0] = true;
            }
        };

        RecordReaders.skipping(src, 1).close();
        assertTrue("skipping 未委派 close", closed[0]);

        closed[0] = false;
        RecordReaders.filtering(src, s -> true).close();
        assertTrue("filtering 未委派 close", closed[0]);

        closed[0] = false;
        RecordReaders.headerAware(src, s -> true).close();
        assertTrue("headerAware 未委派 close", closed[0]);

        closed[0] = false;
        RecordReaders.footerAware(src, s -> true).close();
        assertTrue("footerAware 未委派 close", closed[0]);

        closed[0] = false;
        RecordReaders.mapping(src, s -> s).close();
        assertTrue("mapping 未委派 close", closed[0]);
    }

    @Test
    public void headerAndFooterAwareStack() throws IOException {
        List<String> lines = Arrays.asList("#meta", "name,qty", "A,1", "B,2", "TOTAL 3");
        RecordReader<String> r = RecordReaders.footerAware(
                RecordReaders.headerAware(RecordReaders.fromIterable(lines),
                        s -> s.startsWith("#")),
                s -> s.startsWith("TOTAL"));
        assertEquals(Arrays.asList("name,qty", "A,1", "B,2"), drain(r));
    }

    /** 读完之后继续 next() 应当明确报错，而不是返回 null。 */
    @Test
    public void nextAfterExhaustionThrows() throws IOException {
        RecordReader<String> r = RecordReaders.fromIterable(Arrays.asList("a"));
        assertEquals("a", r.next());
        assertFalse(r.hasNext());
        try {
            r.next();
            throw new AssertionError("应当抛出 NoSuchElementException");
        } catch (NoSuchElementException expected) {
            // 预期
        }
    }
}
