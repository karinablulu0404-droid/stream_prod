package com.stream.realtime.lululemon;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.realtime.lululemon.func.MapMergeJsonData;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;


//
public class DbusSyncPostgreSqlOmsSysData2Kafka {

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
                            return JSON.parseObject(jsonStr);
                        } catch (Exception e) {
                            System.err.println("JSON 解析错误: " + jsonStr);
                            return new JSONObject();
                        }
                    })
                    .uid("convert_str_to_json")
                    .name("convertStr2JsonDs");

            // 处理数据并打印
            convertStr2JsonDs
                    .map(new MapMergeJsonData())
                    .uid("process_json_data")
                    .name("processJsonData")
                    .print();

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
}