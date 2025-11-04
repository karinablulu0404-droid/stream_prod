package com.stream.realtime.lululemon;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.core.EnvironmentSettingUtils;
import com.stream.core.KafkaUtils;
import com.stream.realtime.lululemon.func.MapMergeJsonData;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class DbusSyncSqlserverOmsData2Kafka {

    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置检查点 - 必须在其他配置之前
        configureCheckpoints(env);

        // 设置并行度
        env.setParallelism(1);

        // 应用环境设置
        EnvironmentSettingUtils.defaultParameter(env);

        // Kafka 配置
        String bootstrapServers = "172.17.42.124:9092";
        String topicName = "realtime_v3_logs";

        // 处理 Kafka 主题
        handleKafkaTopic(bootstrapServers, topicName);

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode", "initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.put("snapshot.locking.mode", "none");
        debeziumProperties.put("snapshot.fetch.size", 200);

        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("localhost")
                .port(1433)
                .username("sa")
                .password("Wjk19990921.")
                .database("MyAppDB")
                .tableList("dbo.oms_order_dtl")
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> datasourceStream = env.addSource(sqlServerSource);
        SingleOutputStreamOperator<JSONObject> converStr2JsonDs = datasourceStream.map(JSON::parseObject)
                .uid("converStr2JsonDs")
                .name("converStr2JsonDs");
        converStr2JsonDs.print("===>");

        converStr2JsonDs.map(new MapMergeJsonData()).print("=======>");

        converStr2JsonDs
                .map(JSONObject::toString)
                .sinkTo(KafkaUtils.buildKafkaSink(bootstrapServers, topicName))
                .uid("kafkaSink")
                .name("kafkaSink");

        env.disableOperatorChaining();
        env.execute("DbusSyncSqlserverOmsData2Kafka");
    }

    /**
     * 配置检查点
     */
    private static void configureCheckpoints(StreamExecutionEnvironment env) {
        // 明确设置检查点存储为本地文件系统
        String checkpointDir = "file:///tmp/flink-checkpoints";
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(checkpointDir));

        // 启用检查点，间隔60秒
        env.enableCheckpointing(60000);

        // 设置检查点模式为精确一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // 设置同时进行的检查点数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 设置检查点之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // 设置可容忍的检查点失败次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        // 作业取消时保留检查点
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 配置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
    }

    /**
     * 处理 Kafka 主题：如果存在则删除，然后创建新主题
     */
    private static void handleKafkaTopic(String bootstrapServers, String topicName) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        try (AdminClient adminClient = AdminClient.create(props)) {
            // 检查主题是否存在
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            Set<String> topics = listTopicsResult.names().get();

            if (topics.contains(topicName)) {
                System.out.println("Topic '" + topicName + "' exists, deleting it...");

                // 删除主题
                DeleteTopicsResult deleteResult = adminClient.deleteTopics(Collections.singletonList(topicName));
                deleteResult.all().get(30, TimeUnit.SECONDS);

                System.out.println("Topic '" + topicName + "' deleted successfully");

                // 等待一段时间确保主题被完全删除
                Thread.sleep(2000);
            }

            // 创建主题
            System.out.println("Creating topic '" + topicName + "'...");
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1); // 1个分区，1个副本
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get(30, TimeUnit.SECONDS);
            System.out.println("Topic '" + topicName + "' created successfully");

        } catch (ExecutionException | InterruptedException e) {
            System.err.println("Error handling Kafka topic: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}