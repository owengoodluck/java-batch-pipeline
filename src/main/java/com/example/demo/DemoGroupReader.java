package com.example.demo;

import com.example.batch.RecordReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.NoSuchElementException;

/**
 * 分组演示的读取器：逐行读取 "KEY\tPAYLOAD"，解析成 {@link DemoGroupRecord}。
 * 跳过空行与无法解析的非法行（对齐 {@link DemoLineReader} 的预读模式）。
 */
public class DemoGroupReader implements RecordReader<DemoGroupRecord> {

    private final BufferedReader reader;
    private DemoGroupRecord pending;
    private boolean exhausted;

    public DemoGroupReader(File file, Charset charset) throws IOException {
        this.reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
    }

    public boolean hasNext() throws IOException {
        ensurePending();
        return pending != null;
    }

    public DemoGroupRecord next() throws IOException {
        ensurePending();
        if (pending == null) {
            throw new NoSuchElementException("已读完");
        }
        DemoGroupRecord value = pending;
        pending = null;
        return value;
    }

    /** 预读一条可解析的记录缓存起来，用于支撑 hasNext()；顺便跳过空行与非法行。 */
    private void ensurePending() throws IOException {
        if (pending != null || exhausted) {
            return;
        }
        String line = reader.readLine();
        while (line != null) {
            DemoGroupRecord rec = DemoGroupRecord.fromLine(line);
            if (rec != null) {
                pending = rec;
                return;
            }
            line = reader.readLine();
        }
        exhausted = true;
    }

    public void close() throws IOException {
        reader.close();
    }
}
