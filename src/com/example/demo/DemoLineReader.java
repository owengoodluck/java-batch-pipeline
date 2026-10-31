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
 * 文本文件读取器（RecordReader 的一个具体实现）。
 *
 * <p>演示"按文件类型替换读取实现"：换成 Excel / CSV / 数据库游标时，
 * 只要实现同样的 hasNext()/next() 契约即可，内核无需改动。
 */
public class DemoLineReader implements RecordReader<String> {

    private final BufferedReader reader;
    private String pending;
    private boolean exhausted;

    public DemoLineReader(File file, Charset charset) throws IOException {
        this.reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
    }

    public boolean hasNext() throws IOException {
        ensurePending();
        return pending != null;
    }

    public String next() throws IOException {
        ensurePending();
        if (pending == null) {
            throw new NoSuchElementException("已读完");
        }
        String value = pending;
        pending = null;
        return value;
    }

    /** 预读一条缓存，用于支撑 hasNext()；顺便跳过空行。 */
    private void ensurePending() throws IOException {
        if (pending != null || exhausted) {
            return;
        }
        String line = reader.readLine();
        while (line != null && line.trim().isEmpty()) {
            line = reader.readLine();
        }
        if (line == null) {
            exhausted = true;
        } else {
            pending = line;
        }
    }

    public void close() throws IOException {
        reader.close();
    }
}
