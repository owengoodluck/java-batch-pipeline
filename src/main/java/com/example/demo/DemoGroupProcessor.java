package com.example.demo;

import com.example.batch.BatchFailedException;
import com.example.batch.BatchProcessor;
import com.example.batch.ItemResult;

import java.util.ArrayList;
import java.util.List;

/**
 * 分组演示的批处理器：模拟「把同一批记录发给该组的专属下游接口」。
 *
 * <p>内置分组保证：每次 process() 收到的 batch 只含同一个分组键——这里直接<b>断言</b>这一点，
 * 若框架出错（出现跨组批次）会立即抛 {@link BatchFailedException}，作为对「单组保证」的硬性验证。
 */
public class DemoGroupProcessor implements BatchProcessor<DemoGroupRecord, String> {

    public List<ItemResult<String>> process(List<DemoGroupRecord> batch, long batchSeq)
            throws BatchFailedException {
        if (batch.isEmpty()) {
            return new ArrayList<ItemResult<String>>(0);
        }
        // 单组保证的硬性校验：整批必须是同一个 key
        String group = batch.get(0).getKey();
        for (DemoGroupRecord rec : batch) {
            if (!group.equals(rec.getKey())) {
                throw new BatchFailedException("GROUP_VIOLATION",
                        "单组保证被破坏：批次内同时出现组 " + group + " 与 " + rec.getKey());
            }
        }
        List<ItemResult<String>> out = new ArrayList<ItemResult<String>>(batch.size());
        for (DemoGroupRecord rec : batch) {
            // 成功：code 用 "OK"，payload 透传原负载
            out.add(ItemResult.success("OK", rec.getPayload()));
        }
        System.out.println("[processor] 单组批次: group=" + group + " 共 " + batch.size()
                + " 条，整批发送给该组专属接口");
        return out;
    }
}
