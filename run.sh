#!/usr/bin/env bash
# 编译并运行 Demo（纯 JDK，无需 Maven / 第三方依赖）
set -e
cd "$(dirname "$0")"

OUT="out/classes"
rm -rf "$OUT" demo-run
mkdir -p "$OUT"

echo "==> 编译"
javac -encoding UTF-8 -d "$OUT" $(find src -name "*.java")

echo "==> 运行（默认：专用 RecordReader）"
java -cp "$OUT" com.example.demo.DemoMain

echo
echo "==> 再跑一次（演示：用适配器接入 read() 形态的读取工具）"
java -cp "$OUT" com.example.demo.DemoMain demo-run adapter

echo
echo "==> 产物：demo-run/output/sample.success 与 demo-run/output/sample.error"
