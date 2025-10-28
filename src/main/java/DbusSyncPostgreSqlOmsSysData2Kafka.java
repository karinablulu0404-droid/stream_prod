package com.stream.realtime.lululemon;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.core.EnvironmentSettingUtils;
import com.stream.realtime.lululemon.func.MapMergeJsonData;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class DbusSyncPostgreSqlOmsSysData2Kafka {

    private static final String OMS_OREDR_INFO_REALTIME_ORIGIN_TOPIC = "realtime_v3_order_info";

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode", "initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.put("snapshot.locking.mode", "none");
        debeziumProperties.put("snapshot.fetch.size", 200);
        // 添加 PostgreSQL 特定的配置
        debeziumProperties.put("slot.name", "flink_cdc_slot"); // 复制槽名称
        debeziumProperties.put("publication.name", "flink_cdc_publication"); // 发布名称
        debeziumProperties.put("decoding.plugin.name", "pgoutput"); // 使用 pgoutput 解码插件

        // 使用 PostgreSQLSource 而不是 SqlServerSource
        DebeziumSourceFunction<String> postgresSource = PostgreSQLSource.<String>builder()
                .hostname("172.17.55.4")
                .port(5432)
                .username("postgres")
                .password("Hth1028,./")
                .database("spider_db")
                .schemaList("public") // 指定 schema
                .tableList("public.*") // 监听 public schema 下的所有表，或指定具体表如 "public.table1", "public.table2"
                .decodingPluginName("pgoutput") // PostgreSQL 逻辑解码插件
                .slotName("flink_cdc_slot") // 复制槽名称
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        DataStreamSource<String> dataStreamSource = env.addSource(postgresSource, "_transaction_log_source1");
        SingleOutputStreamOperator<JSONObject> convertStr2JsonDs = dataStreamSource.map(JSON::parseObject)
                .uid("convertStr2JsonDs")
                .name("convertStr2JsonDs");


        convertStr2JsonDs.map(new MapMergeJsonData()).print();






        env.execute("DbusSyncPostgresqlOmsSysData2Kafka");
    }
}