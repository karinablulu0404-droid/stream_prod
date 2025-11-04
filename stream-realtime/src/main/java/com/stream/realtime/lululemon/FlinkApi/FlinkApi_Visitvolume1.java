package com.stream.realtime.lululemon.FlinkApi;

import com.alibaba.fastjson2.JSONObject;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class FlinkApi_Visitvolume1 {

    private static final String OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC = "realtime_v3_logs";
    private static final String KAFKA_BOTSTRAP_SERVERS = "172.17.42.124:9092";

    @SneakyThrows
    public static void main(String[] args) {

        // 首先检查Kafka连接和主题
        checkKafkaConnection();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.fromSource(
                KafkaUtils.buildKafkaSource(KAFKA_BOTSTRAP_SERVERS, OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC,
                        "flink-dbus-log-etl-group", OffsetsInitializer.latest()),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5L),
                "_log_kafka_source_realtime_v3_logs"
        );

        // 添加调试输出
        source.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println("=== 收到Kafka数据 ===");
                System.out.println("原始数据: " + value);
                System.out.println("数据长度: " + value.length());
                return value;
            }
        }).name("debug-raw-data");

        // 主要处理逻辑
        source.flatMap(new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String s, Collector<JSONObject> collector)  {
                        try {
                            System.out.println("开始处理数据: " + s.substring(0, Math.min(s.length(), 100)) + "...");

                            JSONObject jsonObject = JSONObject.parseObject(s);

                            // 验证必要字段
                            Long ts = jsonObject.getLong("ts");
                            String logType = jsonObject.getString("log_type");

                            if (ts == null) {
                                System.out.println("❌ 缺失ts字段");
                                return;
                            }

                            if (logType == null) {
                                System.out.println("❌ 缺失log_type字段");
                                return;
                            }

                            System.out.println("✅ 字段验证通过 - ts: " + ts + ", log_type: " + logType);

                            // ✅ 判断是秒还是毫秒
                            if (ts < 1000000000000L) { // 小于 1 万亿说明是秒级
                                ts = ts * 1000;
                                System.out.println("🕒 时间戳转换为毫秒: " + ts);
                            }

                            // 转为日期字符串（本地时区）
                            LocalDate localDate = Instant.ofEpochMilli(ts)
                                    .atZone(ZoneId.of("Asia/Shanghai"))
                                    .toLocalDate();

                            jsonObject.put("log_date", localDate.toString());
                            collector.collect(jsonObject);

                            System.out.println("✅ 成功处理 - 日期: " + localDate + ", 类型: " + logType);

                        } catch (Exception e) {
                            System.err.println("❌ 数据处理错误: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                })
                .map(new MapFunction<JSONObject, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(JSONObject jsonObject)  {
                        String logDate = jsonObject.getString("log_date");
                        String logType = jsonObject.getString("log_type");
                        System.out.println("📊 统计处理 - 日期: " + logDate + ", 类型: " + logType);
                        return Tuple3.of(logDate, logType, 1L);
                    }
                })
                .keyBy(t -> t.f0 + "_" + t.f1)
                .timeWindow(Time.days(1))
                .sum(2)
                .map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value)  {
                        String result = String.format("🎯 最终结果 - 日期: %s, 页面: %s, PV: %d", value.f0, value.f1, value.f2);
                        System.out.println(result);
                        return result;
                    }
                })
                .print().name("result-output");

        System.out.println("🚀 开始执行Flink作业...");
        env.execute("DbusLogETLMetricTask");
    }

    /**
     * 检查Kafka连接和主题
     */
    private static void checkKafkaConnection() {
        System.out.println("🔍 检查Kafka连接...");

        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BOTSTRAP_SERVERS);
        props.put("request.timeout.ms", 5000);

        try (AdminClient adminClient = AdminClient.create(props)) {
            // 检查连接
            System.out.println("✅ Kafka AdminClient创建成功");

            // 列出所有主题
            ListTopicsResult topicsResult = adminClient.listTopics();
            Set<String> topics = topicsResult.names().get();

            System.out.println("📋 发现 " + topics.size() + " 个主题:");
            for (String topic : topics) {
                System.out.println("   - " + topic);
            }

            // 检查目标主题是否存在
            if (topics.contains(OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC)) {
                System.out.println("✅ 目标主题 '" + OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC + "' 存在");
            } else {
                System.out.println("❌ 目标主题 '" + OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC + "' 不存在");
                System.out.println("可用主题: " + topics);
            }

        } catch (Exception e) {
            System.err.println("❌ Kafka连接检查失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}