//package com.stream.realtime.lululemon;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class DbusSyncPostgreSqlOmsSysData2Kafka {

    private static final String OMS_OREDR_INFO_REALTIME_ORIGIN_TOPIC = "realtime_v3_order_info";

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 替换 EnvironmentSettingUtils.defaultParameter(env)
        setupFlinkEnvironment(env);

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode", "initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.put("snapshot.locking.mode", "none");
        debeziumProperties.put("snapshot.fetch.size", 200);
        // 添加 PostgreSQL 特定的配置
        debeziumProperties.put("slot.name", "flink_cdc_slot"); // 复制槽名称
        debeziumProperties.put("publication.name", "flink_cdc_publication"); // 发布名称
        debeziumProperties.put("decoding.plugin.name", "pgoutput"); // 使用 pgoutput 解码插件

        // 使用 PostgreSQLSource
        DebeziumSourceFunction<String> postgresSource = PostgreSQLSource.<String>builder()
                .hostname("172.17.55.4")
                .port(5432)
                .username("postgres")
                .password("Hth1028,./")
                .database("spider_db")
                .schemaList("public") // 指定 schema
                .tableList("public.*") // 监听 public schema 下的所有表
                .decodingPluginName("pgoutput") // PostgreSQL 逻辑解码插件
                .slotName("flink_cdc_slot") // 复制槽名称
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(postgresSource, "_transaction_log_source1");
        SingleOutputStreamOperator<JSONObject> convertStr2JsonDs = dataStreamSource.map(JSON::parseObject)
                .uid("convertStr2JsonDs")
                .name("convertStr2JsonDs");

        // 替换 MapMergeJsonData
        convertStr2JsonDs.map(new MapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject value) throws Exception {
                return processCDCData(value);
            }
        }).print();

        env.execute("DbusSyncPostgresqlOmsSysData2Kafka");
    }

    private static void setupFlinkEnvironment(StreamExecutionEnvironment env) {
        // Flink 配置
        Configuration config = new Configuration();
        config.setInteger("rest.port", 8081);
        config.setBoolean("local.start-webserver", true);

        // 检查点配置
        env.enableCheckpointing(10000); // 10秒检查点间隔
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 其他默认配置
        env.setParallelism(1);
    }

    private static String processCDCData(JSONObject value) {
        if (value == null) {
            return "No data";
        }

        // 处理 Debezium CDC 数据格式
        StringBuilder result = new StringBuilder();

        // 提取操作类型
        String op = value.getString("op");
        String operation = "";
        switch (op) {
            case "c": operation = "INSERT"; break;
            case "r": operation = "READ"; break;
            case "u": operation = "UPDATE"; break;
            case "d": operation = "DELETE"; break;
            default: operation = "UNKNOWN";
        }

        result.append("Operation: ").append(operation);

        // 提取表信息
        JSONObject source = value.getJSONObject("source");
        if (source != null) {
            String table = source.getString("table");
            String database = source.getString("db");
            result.append(" | Table: ").append(table);
            result.append(" | Database: ").append(database);
        }

        // 提取数据
        if ("c".equals(op) || "r".equals(op)) {
            // 插入或读取
            JSONObject after = value.getJSONObject("after");
            if (after != null) {
                result.append(" | Data: ").append(after.toString());
            }
        } else if ("u".equals(op)) {
            // 更新
            JSONObject before = value.getJSONObject("before");
            JSONObject after = value.getJSONObject("after");
            result.append(" | Before: ").append(before != null ? before.toString() : "null");
            result.append(" | After: ").append(after != null ? after.toString() : "null");
        } else if ("d".equals(op)) {
            // 删除
            JSONObject before = value.getJSONObject("before");
            result.append(" | Deleted: ").append(before != null ? before.toString() : "null");
        }

        // 添加时间戳
        Long ts = value.getLong("ts_ms");
        if (ts != null) {
            result.append(" | Timestamp: ").append(ts);
        }

        return result.toString();
    }
}