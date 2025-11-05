package com.stream.realtime.lululemon.FlinkApi;

import com.alibaba.fastjson2.JSONObject;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class FlinkApi_Visitvolume1 {

    private static final Logger logger = LoggerFactory.getLogger(FlinkApi_Visitvolume1.class);

    private static final String OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC = "realtime_v3_logs";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "172.17.42.124:9092";
    private static final String CONSUMER_GROUP = "flink-dbus-log-etl-group";
    private static final String TIME_ZONE = "Asia/Shanghai";
    private static final long TIMESTAMP_THRESHOLD = 1000000000000L; // 用于判断秒/毫秒的时间戳阈值

    @SneakyThrows
    public static void main(String[] args) {
        // 首先检查Kafka连接和主题
        checkKafkaConnection();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 启用checkpoint（如果需要状态容错）
        env.enableCheckpointing(30000); // 30秒一次
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStreamSource<String> source = env.fromSource(
                KafkaUtils.buildKafkaSource(KAFKA_BOOTSTRAP_SERVERS, OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC,
                        CONSUMER_GROUP, OffsetsInitializer.latest()),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5L),
                "_log_kafka_source_realtime_v3_logs"
        );

        // 添加调试输出
        SingleOutputStreamOperator<String> debugStream = source.map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        logger.debug("=== 收到Kafka数据 ===");
                        logger.debug("原始数据: {}", value);
                        logger.debug("数据长度: {}", value.length());
                        return value;
                    }
                })
                .name("debug-raw-data");

        // 主要处理逻辑
        SingleOutputStreamOperator<JSONObject> parsedStream = debugStream.flatMap(new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String s, Collector<JSONObject> collector) {
                        try {
                            logger.debug("开始处理数据: {}...", s.substring(0, Math.min(s.length(), 100)));

                            JSONObject jsonObject = JSONObject.parseObject(s);

                            // 验证JSON有效性
                            if (jsonObject == null || jsonObject.isEmpty()) {
                                logger.error("❌ 无效的JSON数据: {}", s);
                                return;
                            }

                            // 验证必要字段
                            Long ts = jsonObject.getLong("ts");
                            String logType = jsonObject.getString("log_type");

                            if (ts == null) {
                                logger.error("❌ 缺失ts字段");
                                return;
                            }

                            if (logType == null || logType.trim().isEmpty()) {
                                logger.error("❌ 缺失或空的log_type字段");
                                return;
                            }

                            logger.debug("✅ 字段验证通过 - ts: {}, log_type: {}", ts, logType);

                            // ✅ 判断是秒还是毫秒
                            long processedTs = ts;
                            if (ts < TIMESTAMP_THRESHOLD) { // 小于 1 万亿说明是秒级
                                processedTs = ts * 1000;
                                logger.debug("🕒 时间戳转换为毫秒: {}", processedTs);
                            }

                            // 转为日期字符串（本地时区）
                            LocalDate localDate = Instant.ofEpochMilli(processedTs)
                                    .atZone(ZoneId.of(TIME_ZONE))
                                    .toLocalDate();

                            jsonObject.put("log_date", localDate.toString());
                            jsonObject.put("processed_ts", processedTs);
                            collector.collect(jsonObject);

                            logger.debug("✅ 成功处理 - 日期: {}, 类型: {}", localDate, logType);

                        } catch (Exception e) {
                            logger.error("❌ 数据处理错误, 原始数据: {}", s.substring(0, Math.min(s.length(), 200)), e);
                        }
                    }
                })
                .name("json-parser");

        // 数据过滤和清洗
        SingleOutputStreamOperator<JSONObject> filteredStream = parsedStream.filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String logType = jsonObject.getString("log_type");
                        // 过滤掉无效的日志类型
                        boolean isValid = logType != null && !logType.trim().isEmpty()
                                && !"unknown".equalsIgnoreCase(logType)
                                && !"test".equalsIgnoreCase(logType);

                        if (!isValid) {
                            logger.warn("⚠️ 过滤掉无效日志类型: {}", logType);
                        }
                        return isValid;
                    }
                })
                .name("data-filter");

        // 统计处理
        SingleOutputStreamOperator<String> resultStream = filteredStream.map(new MapFunction<JSONObject, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(JSONObject jsonObject) throws Exception {
                        String logDate = jsonObject.getString("log_date");
                        String logType = jsonObject.getString("log_type");
                        logger.debug("📊 统计处理 - 日期: {}, 类型: {}", logDate, logType);
                        return Tuple3.of(logDate, logType, 1L);
                    }
                })
                .name("statistics-mapper")
                .keyBy(t -> t.f0 + "_" + t.f1)
                .timeWindow(Time.days(1))
                .allowedLateness(Time.minutes(5)) // 允许5分钟延迟
                .sum(2)
                .map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value) throws Exception {
                        String result = String.format("🎯 最终结果 - 日期: %s, 页面: %s, PV: %d", value.f0, value.f1, value.f2);
                        logger.info(result);
                        return result;
                    }
                })
                .name("result-formatter");

        resultStream.print().name("result-output");

        logger.info("🚀 开始执行Flink作业: DbusLogETLMetricTask");
        try {
            env.execute("DbusLogETLMetricTask");
        } catch (Exception e) {
            logger.error("❌ Flink作业执行失败", e);
            throw e;
        }
    }

    /**
     * 检查Kafka连接和主题
     */
    private static void checkKafkaConnection() {
        logger.info("🔍 检查Kafka连接...");

        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        props.put("request.timeout.ms", 10000); // 增加到10秒超时
        props.put("connections.max.idle.ms", 10000);

        try (AdminClient adminClient = AdminClient.create(props)) {
            // 检查连接
            logger.info("✅ Kafka AdminClient创建成功");

            // 列出所有主题
            ListTopicsResult topicsResult = adminClient.listTopics();
            Set<String> topics = topicsResult.names().get();

            logger.info("📋 发现 {} 个主题:", topics.size());
            for (String topic : topics) {
                if (topic.contains("realtime") || topic.contains("log")) {
                    logger.info("   - {} *", topic);
                } else {
                    logger.debug("   - {}", topic);
                }
            }

            // 检查目标主题是否存在
            if (topics.contains(OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC)) {
                logger.info("✅ 目标主题 '{}' 存在", OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC);
            } else {
                logger.error("❌ 目标主题 '{}' 不存在", OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC);
                logger.info("相关主题: {}", topics.stream()
                        .filter(t -> t.contains("realtime") || t.contains("log"))
                        .collect(Collectors.toList()));
            }

        } catch (Exception e) {
            logger.error("❌ Kafka连接检查失败: {}", e.getMessage(), e);
            throw new RuntimeException("Kafka连接失败", e);
        }
    }
}