package com.example.batch;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 默认攒批策略：按到达顺序累积，达到 batchSize 即作为一个批次就绪。
 * 这是未配置 groupBy 时的标准成批方式。
 */
public class CountBatchingStrategyTest {

    @Test
    public void flushExactlyAtBatchSize() {
        CountBatchingStrategy<String> s = new CountBatchingStrategy<String>(3);
        assertTrue(s.drainReadyBatches().isEmpty());
        s.add("a");
        s.add("b");
        assertTrue(s.drainReadyBatches().isEmpty());
        s.add("c");

        List<String> batch = drainOne(s);
        assertEquals(3, batch.size());
        assertEquals("a", batch.get(0));
        assertEquals("c", batch.get(2));
        assertTrue(s.drainReadyBatches().isEmpty());
    }

    @Test
    public void remainderDrainedAtEnd() {
        CountBatchingStrategy<String> s = new CountBatchingStrategy<String>(3);
        s.add("a");
        s.add("b");
        assertTrue(s.drainReadyBatches().isEmpty());

        List<List<String>> all = s.drainAll();
        assertEquals(1, all.size());
        assertEquals(2, all.get(0).size());
    }

    @Test
    public void multipleFullBatches() {
        CountBatchingStrategy<String> s = new CountBatchingStrategy<String>(2);
        s.add("1");
        s.add("2");   // 第 1 批就绪
        assertEquals(2, drainOne(s).size());   // [1, 2]
        assertTrue(s.drainReadyBatches().isEmpty());

        s.add("3");
        s.add("4");   // 第 2 批就绪
        assertEquals(2, drainOne(s).size());   // [3, 4]
        assertTrue(s.drainReadyBatches().isEmpty());
        assertTrue(s.drainAll().isEmpty());
    }

    @Test
    public void emptyDrainAllIsEmpty() {
        CountBatchingStrategy<String> s = new CountBatchingStrategy<String>(5);
        assertTrue(s.drainAll().isEmpty());
    }

    /** 攒了很多条才来取时，也要按 batchSize 切片，不能交出超长批次。 */
    @Test
    public void slicesOversizedPendingWhenDrainedLate() {
        CountBatchingStrategy<String> s = new CountBatchingStrategy<String>(2);
        for (int i = 1; i <= 5; i++) {
            s.add("r" + i);
        }
        List<List<String>> ready = s.drainReadyBatches();
        assertEquals(2, ready.size());
        assertEquals(Arrays.asList("r1", "r2"), ready.get(0));
        assertEquals(Arrays.asList("r3", "r4"), ready.get(1));

        List<List<String>> rest = s.drainAll();
        assertEquals(1, rest.size());
        assertEquals(Arrays.asList("r5"), rest.get(0));
    }

    /** 取出唯一的就绪批次，顺带断言「一次只就绪一个」。 */
    private static <T> List<T> drainOne(BatchingStrategy<T> s) {
        List<List<T>> ready = s.drainReadyBatches();
        assertEquals(1, ready.size());
        return ready.get(0);
    }
}
