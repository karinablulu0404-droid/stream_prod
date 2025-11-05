package com.stream.realtime.lululemon.func;

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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaToDoris {

    public static void main(String[] args) throws Exception {
        // 首先检查Kafka数据
        System.out.println("=== STEP 0: Checking Kafka Data ===");
        checkKafkaData();

        // 然后继续原有的逻辑
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

    /**
     * 检查Kafka数据 - 简化版本，不依赖JSON库
     */
    public static void checkKafkaData() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.42.124:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-data-checker-" + System.currentTimeMillis());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("realtime_v3_logs"));

        System.out.println("=== STARTING KAFKA DATA CHECK ===");
        System.out.println("Topic: realtime_v3_logs");
        System.out.println("Bootstrap servers: 172.17.42.124:9092");
        System.out.println("==================================");

        try {
            // 尝试获取数据，最多等待10秒
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

            if (records.isEmpty()) {
                System.out.println("❌ No messages found in Kafka topic!");
                System.out.println("Possible reasons:");
                System.out.println("1. Topic is empty");
                System.out.println("2. Network connection issue");
                System.out.println("3. Kafka cluster is down");
                System.out.println("4. Authentication required");

                // 检查topic是否存在
                try {
                    System.out.println("Available topics: " + consumer.listTopics().keySet());
                } catch (Exception e) {
                    System.out.println("Cannot list topics: " + e.getMessage());
                }
            } else {
                System.out.println("✅ Found " + records.count() + " messages:");
                System.out.println("==================================");

                int count = 0;
                for (ConsumerRecord<String, String> record : records) {
                    if (count >= 5) break; // 只显示前5条

                    System.out.println("📨 Message " + (++count));
                    System.out.println("Offset: " + record.offset());
                    System.out.println("Partition: " + record.partition());
                    System.out.println("Timestamp: " + record.timestamp());
                    System.out.println("Value: " + record.value());
                    System.out.println("Length: " + record.value().length() + " characters");

                    // 简单的JSON格式检查（不依赖外部库）
                    boolean isValidJson = isLikelyJson(record.value());
                    System.out.println("JSON Format: " + (isValidJson ? "✅ Likely Valid" : "❌ Likely Invalid"));

                    if (isValidJson) {
                        // 简单的字段检查
                        checkFieldsSimple(record.value());
                    }
                    System.out.println("----------------------------------");
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Error consuming from Kafka: " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.close();
            System.out.println("Kafka consumer closed.");
        }
        System.out.println("=== KAFKA DATA CHECK COMPLETED ===");
    }

    /**
     * 简单的JSON格式检查
     */
    private static boolean isLikelyJson(String value) {
        if (value == null || value.trim().isEmpty()) {
            return false;
        }
        String trimmed = value.trim();
        return (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
                (trimmed.startsWith("[") && trimmed.endsWith("]"));
    }

    /**
     * 简单的字段检查
     */
    private static void checkFieldsSimple(String jsonStr) {
        String[] expectedFields = {"log_id", "device", "gis", "network", "opa", "log_type", "ts", "product_id", "order_id", "user_id"};

        System.out.println("Field presence check:");
        for (String field : expectedFields) {
            // 简单的字符串包含检查
            if (jsonStr.contains("\"" + field + "\":")) {
                System.out.println("  ✅ " + field + ": Present");
            } else {
                System.out.println("  ❌ " + field + ": MISSING");
            }
        }
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

                // 等待作业执行一段时间
                Thread.sleep(30000);
                System.out.println("Main stream has been running for 30 seconds");
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
                .setStartingOffsets(OffsetsInitializer.earliest())
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
        props.setProperty("auto.offset.reset", "earliest");
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
        System.out.println("Doris sink table created successfully");
    }
}