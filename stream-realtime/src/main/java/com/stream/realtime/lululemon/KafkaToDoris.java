package com.stream.realtime.lululemon;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.Properties;

public class KafkaToDoris {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
        env.enableCheckpointing(30000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));

        // 设置更详细的日志级别
        env.getConfig().enableClosureCleaner();

        String bootstrapServers = "172.17.42.124:9092";
        String topicName = "realtime_v3_logs";
        String groupId = "kafka-to-doris-consumer-group";

        DataStream<String> kafkaStream = createKafkaSource(env, bootstrapServers, topicName, groupId);

        // 添加详细的调试信息
        DataStream<String> debugStream = kafkaStream
                .map(value -> {
                    System.out.println("=== KAFKA MESSAGE RECEIVED ===");
                    System.out.println("Raw message: " + value);
                    System.out.println("Message length: " + value.length());
                    System.out.println("==============================");
                    return value;
                })
                .name("DebugPrint");

        // 将DataStream注册为临时视图
        tableEnv.createTemporaryView("kafka_source_table", debugStream);

        // 创建Doris Sink表
        createDorisSinkTable(tableEnv);

        // 执行数据同步 - 使用新的调试版本
        executeDataSyncWithDebug(tableEnv);

        System.out.println("=== Starting Flink job execution ===");
        env.execute("KafkaToDorisSync");
    }

    private static void executeDataSyncWithDebug(StreamTableEnvironment tableEnv) throws Exception {
        System.out.println("=== STEP 1: Creating Kafka source table ===");

        // 使用 StringBuilder 避免格式问题
        StringBuilder kafkaTableSql = new StringBuilder();
        kafkaTableSql.append("CREATE TEMPORARY TABLE kafka_source_parsed (")
                .append("    `log_id` STRING,")
                .append("    `device` STRING,")
                .append("    `gis` STRING,")
                .append("    `network` STRING,")
                .append("    `opa` STRING,")
                .append("    `log_type` STRING,")
                .append("    `ts` DOUBLE,")
                .append("    `product_id` STRING,")
                .append("    `order_id` STRING,")
                .append("    `user_id` STRING")
                .append(") WITH (")
                .append("  'connector' = 'kafka',")
                .append("  'topic' = 'realtime_v3_logs',")
                .append("  'properties.bootstrap.servers' = '172.17.42.124:9092',")
                .append("  'properties.group.id' = 'kafka-to-doris-consumer-group',")
                .append("  'format' = 'json',")
                .append("  'json.fail-on-missing-field' = 'false',")
                .append("  'json.ignore-parse-errors' = 'true',")
                .append("  'scan.startup.mode' = 'earliest-offset'")
                .append(")");

        tableEnv.executeSql(kafkaTableSql.toString());
        System.out.println("Kafka source table created successfully");

        // 测试 Kafka 数据
        System.out.println("=== STEP 2: Testing Kafka data ===");
        String sampleQuery = "SELECT * FROM kafka_source_parsed LIMIT 3";
        try {
            TableResult sampleResult = tableEnv.executeSql(sampleQuery);
            System.out.println("Kafka data sample:");
            sampleResult.print();
        } catch (Exception e) {
            System.out.println("Error reading Kafka data: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        // 测试 Doris 连接
        System.out.println("=== STEP 3: Testing Doris connection ===");
        try {
            String testQuery = "SELECT 'doris_connection_test' as status FROM doris_sink LIMIT 1";
            TableResult testResult = tableEnv.executeSql(testQuery);
            System.out.println("Doris connection test successful");
            testResult.print();
        } catch (Exception e) {
            System.out.println("Doris connection failed: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        // 先插入一条测试数据
        System.out.println("=== STEP 4: Inserting test record ===");
        String testInsert = "INSERT INTO doris_sink VALUES (" +
                "'test_flink_doris_" + System.currentTimeMillis() + "', " +
                "'{\"brand\":\"test\",\"plat\":\"android\"}', " +
                "'{\"ip\":\"127.0.0.1\"}', " +
                "'{\"net\":\"wifi\"}', " +
                "'test_page', " +
                "'test_type', " +
                "1762130535980.0, " +
                "'test_product_001', " +
                "'test_order_001', " +
                "'test_user_001'" +
                ")";

        try {
            TableResult testInsertResult = tableEnv.executeSql(testInsert);
            System.out.println("Test record insertion submitted");
            // 获取 job client 来监控状态
            if (testInsertResult.getJobClient().isPresent()) {
                JobClient jobClient = testInsertResult.getJobClient().get();
                System.out.println("Test insertion job ID: " + jobClient.getJobID());
            }
        } catch (Exception e) {
            System.out.println("Test insertion failed: " + e.getMessage());
            e.printStackTrace();
        }

        // 等待一段时间让测试数据有机会执行
        Thread.sleep(10000);

        // 开始主数据流
        System.out.println("=== STEP 5: Starting main data stream ===");
        String mainInsert = "INSERT INTO doris_sink " +
                "SELECT log_id, device, gis, network, opa, log_type, ts, product_id, order_id, user_id " +
                "FROM kafka_source_parsed";

        try {
            TableResult mainResult = tableEnv.executeSql(mainInsert);
            System.out.println("Main data stream started successfully");

            if (mainResult.getJobClient().isPresent()) {
                JobClient jobClient = mainResult.getJobClient().get();
                System.out.println("Main stream job ID: " + jobClient.getJobID());
            }
        } catch (Exception e) {
            System.out.println("Main data stream failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static DataStream<String> createKafkaSource(StreamExecutionEnvironment env,
                                                        String bootstrapServers,
                                                        String topicName,
                                                        String groupId) {

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topicName)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())  // 修改这里
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(createKafkaProperties(groupId))
                .build();

        return env.fromSource(source,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                "KafkaSource");
    }

    private static Properties createKafkaProperties(String groupId) {
        Properties props = new Properties();
        props.setProperty("group.id", groupId);
        props.setProperty("auto.offset.reset", "earliest");  // 修改这里
        props.setProperty("enable.auto.commit", "false");
        return props;
    }

    private static void createDorisSinkTable(StreamTableEnvironment tableEnv) {
        String createDorisTable = "CREATE TABLE doris_sink (\n" +
                "    `log_id` STRING,\n" +
                "    `device` STRING,\n" +  // 存储为 JSON 字符串
                "    `gis` STRING,\n" +     // 存储为 JSON 字符串
                "    `network` STRING,\n" + // 存储为 JSON 字符串
                "    `opa` STRING,\n" +
                "    `log_type` STRING,\n" +
                "    `ts` DOUBLE,\n" +
                "    `product_id` STRING,\n" +
                "    `order_id` STRING,\n" +
                "    `user_id` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '172.17.42.124:8030',\n" +
                "  'table.identifier' = 'test.cdc_data',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'Wjk19990921.',\n" +
                "  'sink.properties.format' = 'json',\n" +
                "  'sink.properties.read_json_by_line' = 'true',\n" +
                "  'sink.batch.size' = '1000',\n" +
                "  'sink.batch.interval' = '1s',\n" +
                "  'sink.properties.strip_outer_array' = 'true',\n" +
                "  'sink.enable-delete' = 'false'\n" +
                ")";

        tableEnv.executeSql(createDorisTable);
    }

    private static void executeDataSync(StreamTableEnvironment tableEnv) {
        // 创建Kafka源表
        String createKafkaSourceTable = "CREATE TEMPORARY TABLE kafka_source_parsed (\n" +
                "    `log_id` STRING,\n" +
                "    `device` STRING,\n" +
                "    `gis` STRING,\n" +
                "    `network` STRING,\n" +
                "    `opa` STRING,\n" +
                "    `log_type` STRING,\n" +
                "    `ts` DOUBLE,\n" +
                "    `product_id` STRING,\n" +
                "    `order_id` STRING,\n" +
                "    `user_id` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'realtime_v3_logs',\n" +
                "  'properties.bootstrap.servers' = '172.17.42.124:9092',\n" +
                "  'properties.group.id' = 'kafka-to-doris-consumer-group',\n" +
                "  'format' = 'json',\n" +
                "  'json.fail-on-missing-field' = 'false',\n" +
                "  'json.ignore-parse-errors' = 'true',\n" +
                "  'scan.startup.mode' = 'latest-offset'\n" +
                ")";

        tableEnv.executeSql(createKafkaSourceTable);

        // 1. 首先测试 Kafka 数据源
        System.out.println("=== STEP 1: Testing Kafka data source ===");
        String kafkaCountQuery = "SELECT COUNT(*) as kafka_count FROM kafka_source_parsed";
        try {
            tableEnv.executeSql(kafkaCountQuery).print();
        } catch (Exception e) {
            System.out.println("Kafka source error: " + e.getMessage());
            e.printStackTrace();
        }

        // 2. 测试 Doris 连接
        System.out.println("=== STEP 2: Testing Doris connection ===");
        try {
            // 先尝试查询现有数据
            String dorisCountQuery = "SELECT COUNT(*) as doris_count FROM doris_sink";
            tableEnv.executeSql(dorisCountQuery).print();
            System.out.println("Doris connection successful");
        } catch (Exception e) {
            System.out.println("Doris connection failed: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        // 3. 测试插入单条数据
        System.out.println("=== STEP 3: Testing single record insertion ===");
        String testInsert = "INSERT INTO doris_sink VALUES (" +
                "'test_log_id_123', " +
                "'{\"brand\":\"test\",\"plat\":\"android\"}', " +
                "'{\"ip\":\"127.0.0.1\"}', " +
                "'{\"net\":\"wifi\"}', " +
                "'page', " +
                "'home', " +
                "1762130535980.0, " +
                "'test_product_123', " +
                "'test_order_123', " +
                "'test_user_123'" +
                ")";

        try {
            TableResult testResult = tableEnv.executeSql(testInsert);
            System.out.println("Test insertion submitted");

            // 等待一段时间让测试数据写入
            Thread.sleep(5000);

            // 检查测试数据是否写入
            String checkTestData = "SELECT COUNT(*) as test_count FROM doris_sink WHERE log_id = 'test_log_id_123'";
            tableEnv.executeSql(checkTestData).print();

        } catch (Exception e) {
            System.out.println("Test insertion failed: " + e.getMessage());
            e.printStackTrace();
        }

        // 4. 开始正式的数据同步
        System.out.println("=== STEP 4: Starting main data sync ===");
        String insertIntoSql = "INSERT INTO doris_sink \n" +
                "SELECT \n" +
                "  log_id,\n" +
                "  device,\n" +
                "  gis,\n" +
                "  network,\n" +
                "  opa,\n" +
                "  log_type,\n" +
                "  ts,\n" +
                "  product_id,\n" +
                "  order_id,\n" +
                "  user_id\n" +
                "FROM kafka_source_parsed";

        try {
            TableResult result = tableEnv.executeSql(insertIntoSql);
            System.out.println("=== Main data sync job submitted successfully ===");

            // 定期检查数据写入情况
            for (int i = 0; i < 6; i++) {
                Thread.sleep(10000); // 每10秒检查一次
                String progressQuery = "SELECT COUNT(*) as current_count FROM doris_sink";
                System.out.println("Progress check " + (i + 1) + ":");
                tableEnv.executeSql(progressQuery).print();
            }

        } catch (Exception e) {
            System.out.println("=== Error in main data sync: " + e.getMessage());
            e.printStackTrace();
        }
    }
}