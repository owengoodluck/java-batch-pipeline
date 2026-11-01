package com.example.batch;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 端到端流水线测试：用内存 reader（{@link RecordReaders#fromIterable}）+ 假 processor，
 * 验证最新设计的关键行为——内置分组的「单组保证」、默认计数批处理、成功/错误守恒、批次级失败兜底。
 */
public class BatchPipelineTest {

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

    /** 断言每批单组，并统计每个分组键被处理的条数。 */
    private static final class GroupAssertProcessor implements BatchProcessor<Rec, String> {
        final Map<String, Integer> perKey = Collections.synchronizedMap(new HashMap<String, Integer>());
        final List<Integer> batchSizes = Collections.synchronizedList(new ArrayList<Integer>());

        public List<ItemResult<String>> process(List<Rec> batch, long batchSeq) throws BatchFailedException {
            batchSizes.add(batch.size());
            String g = batch.get(0).getKey();
            for (Rec r : batch) {
                if (!g.equals(r.getKey())) {
                    throw new BatchFailedException("GROUP_VIOLATION", "单组保证被破坏");
                }
            }
            List<ItemResult<String>> out = new ArrayList<ItemResult<String>>(batch.size());
            for (Rec r : batch) {
                perKey.put(g, perKey.getOrDefault(g, 0) + 1);
                out.add(ItemResult.success("OK", r.getPayload()));
            }
            return out;
        }
    }

    private static final class CountingProcessor implements BatchProcessor<Rec, String> {
        int calls = 0;

        public List<ItemResult<String>> process(List<Rec> batch, long batchSeq) {
            calls++;
            List<ItemResult<String>> out = new ArrayList<ItemResult<String>>(batch.size());
            for (Rec r : batch) {
                out.add(ItemResult.success("OK", r.getPayload()));
            }
            return out;
        }
    }

    @Test
    public void groupingSingleGroupGuaranteeAndConservation() throws Exception {
        List<Rec> input = new ArrayList<Rec>();
        String[] groups = {"G1", "G2", "G3"};
        int perGroup = 10;
        for (int i = 0; i < perGroup * 3; i++) {
            input.add(new Rec(groups[i % 3], "p" + i));
        }

        GroupAssertProcessor proc = new GroupAssertProcessor();
        File outDir = tempDir("group-out");
        BatchResult res = BatchPipelines.run(
                PipelineConfig.<Rec, String>builder()
                        .reader(RecordReaders.fromIterable(new ArrayList<Rec>(input)))
                        .processor(proc)
                        .groupBy(new GroupKeyExtractor<Rec, String>() {
                            public String extract(Rec r) {
                                return r.getKey();
                            }
                        })
                        .batchSize(10)
                        .consumerThreads(2)
                        .bufferBatches(2)
                        .charset(StandardCharsets.UTF_8)
                        .channel("success", ChannelTargets.file(new File(outDir, "ok")),
                                (r, result) -> r.getKey() + "\t" + result.getPayload(), true)
                        .errorChannel(ChannelTargets.file(new File(outDir, "err")),
                                (r, result) -> result.getCode() + "\t" + result.getMessage(), true)
                        .build());

        assertEquals(input.size(), res.getReadCount());
        assertEquals(input.size(), res.getRecordSuccess());
        assertEquals((long) input.size(), res.getChannelCounts().get("success").longValue());
        // 每个分组键都被完整处理
        for (String g : groups) {
            assertEquals(perGroup, proc.perKey.get(g).intValue());
        }
        // 每个批次都没越界（单组保证没有被破坏）
        assertTrue(proc.batchSizes.stream().allMatch(s -> s <= 10));
    }

    @Test
    public void defaultCountingSplitsByBatchSize() throws Exception {
        int n = 250;
        List<Rec> input = new ArrayList<Rec>();
        for (int i = 0; i < n; i++) {
            input.add(new Rec("k", "p" + i));
        }
        CountingProcessor proc = new CountingProcessor();
        File outDir = tempDir("count-out");
        BatchResult res = BatchPipelines.run(
                PipelineConfig.<Rec, String>builder()
                        .reader(RecordReaders.fromIterable(input))
                        .processor(proc)
                        .batchSize(100)
                        .consumerThreads(1)
                        .channel("success", ChannelTargets.file(new File(outDir, "ok")),
                                (r, result) -> result.getPayload(), true)
                        .errorChannel(ChannelTargets.file(new File(outDir, "err")),
                                (r, result) -> result.getCode(), true)
                        .build());

        assertEquals(n, res.getReadCount());
        assertEquals(n, res.getRecordSuccess());
        assertEquals(3, proc.calls);   // 100 + 100 + 50 三批
    }

    @Test
    public void errorChannelConservationOnRecordFailure() throws Exception {
        int n = 100;
        List<Rec> input = new ArrayList<Rec>();
        for (int i = 0; i < n; i++) {
            input.add(new Rec("k", "p" + i));
        }
        BatchProcessor<Rec, String> proc = new BatchProcessor<Rec, String>() {
            public List<ItemResult<String>> process(List<Rec> batch, long seq) {
                List<ItemResult<String>> out = new ArrayList<ItemResult<String>>(batch.size());
                for (int i = 0; i < batch.size(); i++) {
                    if (i % 3 == 0) {
                        out.add(ItemResult.failed("BIZ", "bad"));
                    } else {
                        out.add(ItemResult.success("OK", "x"));
                    }
                }
                return out;
            }
        };
        File outDir = tempDir("err-out");
        BatchResult res = BatchPipelines.run(
                PipelineConfig.<Rec, String>builder()
                        .reader(RecordReaders.fromIterable(input))
                        .processor(proc)
                        .batchSize(20)
                        .channel("success", ChannelTargets.file(new File(outDir, "ok")),
                                (r, result) -> "", true)
                        .errorChannel(ChannelTargets.file(new File(outDir, "err")),
                                (r, result) -> "", true)
                        .build());

        assertEquals(n, res.getReadCount());
        assertEquals(res.getRecordSuccess() + res.getRecordFailedTotal(), n);
        assertEquals(res.getRecordSuccess(), (long) res.getChannelCounts().get("success"));
        assertEquals(res.getRecordFailedTotal(), (long) res.getChannelCounts().get("error"));
    }

    @Test
    public void batchFailureWholeBatchToError() throws Exception {
        int n = 50;
        List<Rec> input = new ArrayList<Rec>();
        for (int i = 0; i < n; i++) {
            input.add(new Rec("k", "p" + i));
        }
        BatchProcessor<Rec, String> proc = new BatchProcessor<Rec, String>() {
            public List<ItemResult<String>> process(List<Rec> batch, long seq) throws BatchFailedException {
                throw new BatchFailedException("BATCH_FAIL", "boom");
            }
        };
        File outDir = tempDir("bf-out");
        BatchResult res = BatchPipelines.run(
                PipelineConfig.<Rec, String>builder()
                        .reader(RecordReaders.fromIterable(input))
                        .processor(proc)
                        .batchSize(10)
                        .channel("success", ChannelTargets.file(new File(outDir, "ok")),
                                (r, result) -> "", true)
                        .errorChannel(ChannelTargets.file(new File(outDir, "err")),
                                (r, result) -> "", true)
                        .build());

        assertEquals(n, res.getReadCount());
        assertEquals(0, res.getRecordSuccess());
        assertEquals(n, res.getRecordFailedTotal());
        assertEquals((long) n, res.getChannelCounts().get("error").longValue());
    }

    /** 内存输出目标：用来验证「输出目标」抽象对非文件介质同样成立。 */
    private static final class MemoryTarget implements ChannelTarget {
        final ByteArrayOutputStream staging = new ByteArrayOutputStream();
        String published;
        int openCount = 0;
        int commitCount = 0;

        public OutputStream open() {
            openCount++;
            return staging;
        }

        public void commit() {
            commitCount++;
            published = new String(staging.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    @Test
    public void customChannelTargetReceivesWholeMergedOutput() throws Exception {
        int n = 25;
        List<Rec> input = new ArrayList<Rec>();
        for (int i = 1; i <= n; i++) {
            input.add(new Rec("k", "p" + i));
        }
        MemoryTarget success = new MemoryTarget();
        MemoryTarget errors = new MemoryTarget();

        BatchResult res = BatchPipelines.run(
                PipelineConfig.<Rec, String>builder()
                        .reader(RecordReaders.fromIterable(input))
                        .processor(new CountingProcessor())
                        .batchSize(10)
                        .consumerThreads(1)
                        .charset(StandardCharsets.UTF_8)
                        .channel("success", success, (r, result) -> result.getPayload(), true)
                        .errorChannel(errors, (r, result) -> "", true)
                        .build());

        // 每个通道严格走一次 open → 写入 → close → commit
        assertEquals(1, success.openCount);
        assertEquals(1, success.commitCount);
        assertEquals(1, errors.openCount);
        assertEquals(1, errors.commitCount);

        // 三个分片（10+10+5）按批次序号合并，输出保持输入行序
        String[] lines = success.published.split("\n");
        assertEquals(n, lines.length);
        for (int i = 0; i < n; i++) {
            assertEquals("p" + (i + 1), lines[i]);
        }
        // 无失败记录：错误通道因 createEmpty 产出空结果
        assertEquals("", errors.published);
    }

    private static File tempDir(String name) throws Exception {
        File d = new File(System.getProperty("java.io.tmpdir"), "batch-test-" + name + "-" + System.nanoTime());
        if (!d.mkdirs()) {
            throw new IllegalStateException("无法创建临时目录: " + d);
        }
        return d;
    }
}
