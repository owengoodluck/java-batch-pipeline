package com.example.batch;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * 内置分组攒批策略的核心契约：每次 {@link #drainReadyBatches()} / {@link #drainAll()} 返回的批次
 * 只含同一个分组键（单组保证），且输入结束时各桶未满部分会被兜底 flush（不丢数据）。
 */
public class GroupBatchingStrategyTest {

    private static final class Rec {
        final String key;
        final String payload;

        Rec(String key, String payload) {
            this.key = key;
            this.payload = payload;
        }

        String getKey() {
            return key;
        }

        @SuppressWarnings("unused")
        String getPayload() {
            return payload;
        }
    }

    private GroupKeyExtractor<Rec, String> byKey() {
        return new GroupKeyExtractor<Rec, String>() {
            public String extract(Rec r) {
                return r.getKey();
            }
        };
    }

    /** 断言一个批次内所有记录同组——这是「单组保证」的硬验证。 */
    private void assertSingleGroup(List<Rec> batch) {
        assertFalse("空批次不应被提交", batch.isEmpty());
        String g = batch.get(0).getKey();
        for (Rec r : batch) {
            assertEquals("单组保证被破坏：批次内同时出现 " + g + " 与 " + r.getKey(), g, r.getKey());
        }
    }

    /** 取出唯一的就绪批次（分组场景下同一轮只应有一个桶攒满）。 */
    private static List<Rec> drainOne(BatchingStrategy<Rec> s) {
        List<List<Rec>> ready = s.drainReadyBatches();
        assertEquals(1, ready.size());
        return ready.get(0);
    }

    @Test
    public void singleGroupGuaranteeAcrossInterleavedInput() {
        GroupBatchingStrategy<Rec> s = new GroupBatchingStrategy<Rec>(3, byKey());
        s.add(rec("A", "1"));
        s.add(rec("B", "1"));
        s.add(rec("A", "2"));
        s.add(rec("B", "2"));
        s.add(rec("A", "3"));   // A 达到 3 → 就绪

        List<Rec> aBatch = drainOne(s);
        assertEquals(3, aBatch.size());
        assertSingleGroup(aBatch);

        assertTrue(s.drainReadyBatches().isEmpty());   // B 只有 2 条，未就绪
        s.add(rec("B", "3"));                          // B 达到 3 → 就绪
        assertSingleGroup(drainOne(s));
        assertTrue(s.drainAll().isEmpty());
    }

    @Test
    public void interleavedBelowBatchSizeNeverPrematurelyFlushes() {
        GroupBatchingStrategy<Rec> s = new GroupBatchingStrategy<Rec>(5, byKey());
        for (int i = 0; i < 4; i++) {
            s.add(rec("A", "" + i));
            s.add(rec("B", "" + i));
        }
        // A=4、B=4，都不足 5 → 不就绪
        assertTrue(s.drainReadyBatches().isEmpty());
        s.add(rec("A", "x"));   // A=5 → 就绪，且整批都是 A
        List<Rec> b = drainOne(s);
        assertEquals(5, b.size());
        assertSingleGroup(b);
    }

    @Test
    public void drainAllFlushesRemainingBuckets() {
        GroupBatchingStrategy<Rec> s = new GroupBatchingStrategy<Rec>(10, byKey());
        s.add(rec("X", "1"));
        s.add(rec("X", "2"));
        s.add(rec("Y", "1"));
        s.add(rec("Y", "2"));
        assertTrue(s.drainReadyBatches().isEmpty());

        List<List<Rec>> all = s.drainAll();
        assertEquals(2, all.size());
        for (List<Rec> b : all) {
            assertSingleGroup(b);
        }
    }

    @Test
    public void largeGroupSlicedIntoMultipleSingleGroupBatches() {
        GroupBatchingStrategy<Rec> s = new GroupBatchingStrategy<Rec>(4, byKey());
        final int total = 10;   // 4 + 4 + 2
        for (int i = 0; i < total; i++) {
            s.add(rec("Z", "" + i));
        }
        int seen = 0;
        List<List<Rec>> ready = s.drainReadyBatches();
        assertEquals(2, ready.size());   // 4 + 4
        for (List<Rec> b : ready) {
            seen += b.size();
            assertSingleGroup(b);
        }
        for (List<Rec> b : s.drainAll()) {   // 剩下的 2 条
            seen += b.size();
            assertSingleGroup(b);
        }
        assertEquals(total, seen);
    }

    @Test
    public void nullKeyFormsItsOwnBucket() {
        GroupBatchingStrategy<Rec> s = new GroupBatchingStrategy<Rec>(2,
                new GroupKeyExtractor<Rec, String>() {
                    public String extract(Rec r) {
                        return r.getKey().isEmpty() ? null : r.getKey();
                    }
                });
        s.add(rec("", "1"));
        s.add(rec("", "2"));
        List<Rec> b = drainOne(s);
        assertEquals(2, b.size());
        assertSingleGroup(b);   // 两条都是 null key
    }

    @Test
    public void bucketRefillsAfterFlushAndStaysSingleGroup() {
        GroupBatchingStrategy<Rec> s = new GroupBatchingStrategy<Rec>(2, byKey());
        s.add(rec("A", "1"));
        s.add(rec("A", "2"));
        assertSingleGroup(drainOne(s));    // 第 1 个 A 桶

        s.add(rec("A", "3"));               // 同键再来 → 开新桶
        assertTrue(s.drainReadyBatches().isEmpty());
        s.add(rec("A", "4"));
        List<Rec> second = drainOne(s);
        assertEquals(2, second.size());
        assertSingleGroup(second);
        assertTrue(s.drainAll().isEmpty());
    }

    private Rec rec(String k, String p) {
        return new Rec(k, p);
    }
}
