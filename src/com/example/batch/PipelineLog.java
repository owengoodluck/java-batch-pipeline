package com.example.batch;

/**
 * 极简日志门面：默认输出到 stdout，零第三方依赖，保证 demo 可以直接 javac 跑起来。
 * 接入 SLF4J 时把本类的方法体替换成对应 logger 调用即可，不改任何业务代码。
 */
public final class PipelineLog {

    private PipelineLog() {
    }

    public static void info(String format, Object... args) {
        print("INFO ", format, args);
    }

    public static void warn(String format, Object... args) {
        print("WARN ", format, args);
    }

    public static void error(String format, Object... args) {
        print("ERROR", format, args);
    }

    private static void print(String level, String format, Object[] args) {
        String msg = args == null || args.length == 0 ? format : String.format(format, args);
        System.out.println("[" + level + "] " + Thread.currentThread().getName() + " " + msg);
    }
}
