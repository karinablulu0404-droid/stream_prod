package com.stream.realtime.lululemon.func;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.core.KafkaUtils;
import com.stream.realtime.lululemon.func.MapMergeJsonData;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DbusSyncPostgreSqlOmsSysData2Kafka {

    private static final String KAFKA_BOOTSTRAP_SERVERS = "172.17.42.124:9092";
    private static final String KAFKA_TOPIC = "realtime_v3_logs";

    @SneakyThrows
    public static void main(String[] args) {

        // 设置 Hadoop 相关属性避免冲突
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("hadoop.home.dir", "C:\\");
        System.setProperty("hadoop.tmp.dir", System.getProperty("java.io.tmpdir"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 基础配置
        env.setParallelism(1);
        env.disableOperatorChaining();

        // 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

        // 检查并创建Kafka主题
        ensureKafkaTopicExists();

        // PostgreSQL CDC 配置
        Properties debeziumProperties = new Properties();

        // 基础配置
        debeziumProperties.put("snapshot.mode", "initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.put("snapshot.locking.mode", "none");
        debeziumProperties.put("snapshot.fetch.size", "200");

        // PostgreSQL 特定配置
        debeziumProperties.put("slot.name", "flink_cdc_slot");
        debeziumProperties.put("publication.name", "flink_cdc_publication");
        debeziumProperties.put("decoding.plugin.name", "pgoutput");

        // 连接和重试配置
        debeziumProperties.put("connect.timeout.ms", "10000");
        debeziumProperties.put("heartbeat.interval.ms", "10000");
        debeziumProperties.put("max.batch.size", "2048");
        debeziumProperties.put("max.queue.size", "8192");

        // 错误处理配置
        debeziumProperties.put("errors.log.include.messages", "true");
        debeziumProperties.put("errors.retry.delay.initial.ms", "500");
        debeziumProperties.put("errors.retry.delay.max.ms", "30000");

        try {
            System.out.println("开始创建 PostgreSQL CDC 源...");

            DebeziumSourceFunction<String> postgresSource = PostgreSQLSource.<String>builder()
                    .hostname("127.0.0.1")
                    .port(5432)
                    .username("etl_flink_cdc_pub_user")
                    .password("etl_flink_cdc_pub_user123,./")
                    .database("spider_db")
                    .schemaList("public")
                    .tableList("public.*") // 可以指定具体表，如 "public.table1", "public.table2"
                    .decodingPluginName("pgoutput")
                    .slotName("flink_cdc_slot")
                    .debeziumProperties(debeziumProperties)
                    .deserializer(new JsonDebeziumDeserializationSchema())
                    .build();

            System.out.println("PostgreSQL CDC 源创建成功，开始添加数据流...");

            DataStreamSource<String> dataStreamSource = env.addSource(postgresSource, "postgres_cdc_source")
                    .setParallelism(1);

            SingleOutputStreamOperator<JSONObject> convertStr2JsonDs = dataStreamSource
                    .map(jsonStr -> {
                        try {
                            System.out.println("收到PostgreSQL变更数据: " + jsonStr);
                            return JSON.parseObject(jsonStr);
                        } catch (Exception e) {
                            System.err.println("JSON 解析错误: " + jsonStr);
                            // 返回包含错误信息的JSON对象，而不是空对象
                            JSONObject errorObj = new JSONObject();
                            errorObj.put("error", true);
                            errorObj.put("raw_data", jsonStr);
                            errorObj.put("error_message", e.getMessage());
                            return errorObj;
                        }
                    })
                    .uid("convert_str_to_json")
                    .name("convertStr2JsonDs");

            // 处理数据并打印（用于调试）
            SingleOutputStreamOperator<JSONObject> processedData = convertStr2JsonDs
                    .map(new MapMergeJsonData())
                    .uid("process_json_data")
                    .name("processJsonData");

            // 打印处理后的数据（调试用）
            processedData.print("✅ PostgreSQL处理结果");

            // 添加Kafka Sink - 将数据写入Kafka
            processedData
                    .map(JSONObject::toString) // 将JSONObject转换为字符串
                    .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC))
                    .uid("postgres_kafka_sink")
                    .name("postgresKafkaSink");

            System.out.println("Kafka Sink配置完成，目标主题: " + KAFKA_TOPIC);
            System.out.println("开始执行 Flink 作业...");

            env.execute("DbusSyncPostgreSqlOmsSysData2Kafka");

        } catch (Exception e) {
            System.err.println("作业执行失败: " + e.getMessage());
            e.printStackTrace();

            // 提供具体的错误解决方案
            if (e.getMessage().contains("wal_level") || e.getMessage().contains("逻辑解码")) {
                System.err.println("\n=== PostgreSQL 配置问题解决方案 ===");
                System.err.println("1. 修改 PostgreSQL 配置文件 postgresql.conf:");
                System.err.println("   wal_level = logical");
                System.err.println("   max_replication_slots = 10");
                System.err.println("   max_wal_senders = 10");
                System.err.println("2. 重启 PostgreSQL 服务");
                System.err.println("3. 验证配置: SELECT name, setting FROM pg_settings WHERE name IN ('wal_level', 'max_replication_slots');");
            } else if (e.getMessage().contains("replication slot")) {
                System.err.println("\n=== 复制槽问题解决方案 ===");
                System.err.println("1. 删除现有复制槽: SELECT pg_drop_replication_slot('flink_cdc_slot');");
                System.err.println("2. 重新运行程序创建新的复制槽");
            }
        }
    }

    /**
     * 确保Kafka主题存在
     */
    private static void ensureKafkaTopicExists() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        try (AdminClient adminClient = AdminClient.create(props)) {
            // 检查主题是否存在
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            Set<String> topics = listTopicsResult.names().get();

            if (!topics.contains(KAFKA_TOPIC)) {
                System.out.println("Kafka主题 '" + KAFKA_TOPIC + "' 不存在，正在创建...");

                // 创建主题
                NewTopic newTopic = new NewTopic(KAFKA_TOPIC, 3, (short) 1); // 3个分区，1个副本
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get(30, TimeUnit.SECONDS);

                System.out.println("Kafka主题 '" + KAFKA_TOPIC + "' 创建成功");
            } else {
                System.out.println("Kafka主题 '" + KAFKA_TOPIC + "' 已存在");
            }

        } catch (Exception e) {
            System.err.println("Kafka主题检查/创建失败: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Kafka主题初始化失败", e);
        }
    }
}