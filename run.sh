#!/usr/bin/env bash
# 编译并运行 Demo（基于 Maven，无需手动 javac）
set -e
cd "$(dirname "$0")"

echo "==> Maven 编译"
mvn -q compile

CP="target/classes"

echo
echo "==> DemoMain（专用 RecordReader 实现读取）"
java -cp "$CP" com.example.demo.DemoMain demo-run reader

echo
echo "==> DemoMain（用适配器接入 read() 形态的读取工具）"
java -cp "$CP" com.example.demo.DemoMain demo-run adapter

echo
echo "==> 产物：demo-run/output/sample.success 与 demo-run/output/sample.error"

echo
echo "==> 多通道示例：1000 条订单按查询到的状态分到 4 个业务文件 + 1 个错误文件"
java -cp "$CP" com.example.demo.DemoOrderMain order-demo 1000

echo
echo "==> 产物：order-demo/output/order.{paid,shipped,completed,cancelled,error}"
echo "==> 分片：order-demo/parts/{通道名}/part-000001.part"

echo
echo "==> 内置分组示例：按 key 字段分组，每攒满一批同组记录才调一次下游（单组保证）"
java -cp "$CP" com.example.demo.DemoGroupMain demo-run/group

echo
echo "==> 产物：demo-run/group/output/group.{success,error}"
