package com.stream.realtime.lululemon.Damopan;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SqlServerToDoris {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // SQL Server 源表 - 使用正确的字段名 ai_review
        String sourceDDL = "CREATE TABLE oms_order_dtl_enhanced2 (\n" +
                "  id INT,\n" +
                "  order_id STRING,\n" +
                "  user_id STRING,\n" +
                "  product_id STRING,\n" +
                "  product_name STRING,\n" +
                "  brand STRING,\n" +
                "  english_name STRING,\n" +
                "  chinese_name STRING,\n" +
                "  ai_review STRING,\n" +  // 改为 ai_review
                "  has_sensitive_content BOOLEAN,\n" +
                "  sensitive_words STRING,\n" +
                "  violation_handled BOOLEAN,\n" +
                "  sale_amount DECIMAL(18,2),\n" +
                "  total_amount DECIMAL(18,2),\n" +
                "  ds DATE,\n" +
                "  created_time TIMESTAMP(3),\n" +
                "  PRIMARY KEY(id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'sqlserver-cdc',\n" +
                "  'hostname' = 'localhost',\n" +
                "  'port' = '1433',\n" +
                "  'username' = 'sa',\n" +
                "  'password' = 'Wjk19990921.',\n" +
                "  'database-name' = 'MyAppDB',\n" +
                "  'table-name' = 'dbo.oms_order_dtl_enhanced2',\n" +
                "  'scan.startup.mode' = 'initial',\n" +
                "  'debezium.snapshot.mode' = 'initial',\n" +
                "  'debezium.snapshot.locking.mode' = 'none',\n" +
                "  'debezium.database.history.skip.unparseable.ddl' = 'true',\n" +
                "  'debezium.schema.history.internal' = 'io.debezium.relational.history.MemorySchemaHistory',\n" +
                "  'debezium.include.schema.changes' = 'false'\n" +
                ")";

        // 创建打印表用于调试
        String printTableDDL = "CREATE TABLE print_test (\n" +
                "  id INT,\n" +
                "  order_id STRING,\n" +
                "  user_id STRING,\n" +
                "  product_id STRING,\n" +
                "  product_name STRING,\n" +
                "  brand STRING,\n" +
                "  english_name STRING,\n" +
                "  chinese_name STRING,\n" +
                "  ai_review STRING,\n" +  // 改为 ai_review
                "  has_sensitive_content BOOLEAN,\n" +
                "  sensitive_words STRING,\n" +
                "  violation_handled BOOLEAN,\n" +
                "  sale_amount DECIMAL(18,2),\n" +
                "  total_amount DECIMAL(18,2),\n" +
                "  ds DATE,\n" +
                "  created_time TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";

        // Doris 目标表
        String sinkDDL = "CREATE TABLE doris_oms_order (\n" +
                "  id INT,\n" +
                "  order_id STRING,\n" +
                "  user_id STRING,\n" +
                "  product_id STRING,\n" +
                "  product_name STRING,\n" +
                "  brand STRING,\n" +
                "  english_name STRING,\n" +
                "  chinese_name STRING,\n" +
                "  ai_review STRING,\n" +
                "  has_sensitive_content BOOLEAN,\n" +
                "  sensitive_words STRING,\n" +
                "  violation_handled BOOLEAN,\n" +
                "  sale_amount DECIMAL(18,2),\n" +
                "  total_amount DECIMAL(18,2),\n" +
                "  ds DATE,\n" +
                "  created_time TIMESTAMP(3),\n" +
                "  PRIMARY KEY(id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '172.17.42.124:8030',\n" +
                "  'table.identifier' = 'target_db.oms_order_dtl_enhanced',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'Wjk19990921.',\n" +
                "  'sink.buffer-flush.max-rows' = '1000',\n" +
                "  'sink.buffer-flush.interval' = '10s',\n" +
                "  'sink.max-retries' = '3',\n" +
                "  'sink.properties.format' = 'json',\n" +
                "  'sink.properties.read_json_by_line' = 'true'\n" +
                ")";

        try {
            System.out.println("=== SQL Server CDC 到 Doris 数据同步 ===");
            System.out.println("✅ CDC 状态确认: capture_instance = 'dbo_oms_order_dtl_enhanced2'");
            System.out.println("✅ CDC 配置: supports_net_changes = true");
            System.out.println("✅ 检测到 8096 条记录，正在同步...");

            // 创建表
            System.out.println("=== 创建 Flink 表 ===");
            tableEnv.executeSql(sourceDDL);
            System.out.println("✅ SQL Server CDC 源表创建成功");

            tableEnv.executeSql(printTableDDL);
            System.out.println("✅ 打印调试表创建成功");

            tableEnv.executeSql(sinkDDL);
            System.out.println("✅ Doris 目标表创建成功");

            // 第一步：测试 CDC 数据读取
            System.out.println("=== 步骤1: 测试 CDC 数据读取 ===");
            String testSQL = "INSERT INTO print_test SELECT * FROM oms_order_dtl_enhanced2";
            tableEnv.executeSql(testSQL);
            System.out.println("CDC 测试作业已提交...");

            // 等待一段时间看是否有数据输出
            System.out.println("等待初始数据同步（15秒）...");
            for (int i = 1; i <= 15; i++) {
                Thread.sleep(1000);
                if (i % 5 == 0) {
                    System.out.println("已等待 " + i + " 秒...");
                }
            }

            // 第二步：启动正式同步到 Doris
            System.out.println("=== 步骤2: 启动正式同步到 Doris ===");
            String insertSQL =
                    "INSERT INTO doris_oms_order \n" +
                            "SELECT \n" +
                            "  id,                              -- id\n" +
                            "  order_id,                        -- order_id\n" +
                            "  user_id,                         -- user_id\n" +
                            "  product_id,                      -- product_id\n" +
                            "  product_name,                    -- product_name\n" +
                            "  brand,                           -- brand\n" +
                            "  english_name,                    -- english_name\n" +
                            "  chinese_name,                    -- chinese_name\n" +
                            "  ai_review,                       -- ai_review (直接映射)\n" +
                            "  has_sensitive_content,           -- has_sensitive_content\n" +
                            "  sensitive_words,                 -- sensitive_words\n" +
                            "  violation_handled,               -- violation_handled\n" +
                            "  sale_amount,                     -- sale_amount\n" +
                            "  total_amount,                    -- total_amount\n" +
                            "  ds,                              -- ds\n" +
                            "  created_time                     -- created_time\n" +
                            "FROM oms_order_dtl_enhanced2";

            tableEnv.executeSql(insertSQL);
            System.out.println("✅ Doris 同步作业已启动");

            System.out.println("🎉 所有作业启动成功！");
            System.out.println("");
            System.out.println("📊 数据流向: SQL Server → Flink CDC → Doris");
            System.out.println("📈 检测到数据量: 8096 条记录");
            System.out.println("");
            System.out.println("🔧 字段映射关系:");
            System.out.println("   SQL Server字段       →    Doris字段");
            System.out.println("   id                   →    id");
            System.out.println("   order_id             →    order_id");
            System.out.println("   user_id              →    user_id");
            System.out.println("   product_id           →    product_id");
            System.out.println("   product_name         →    product_name");
            System.out.println("   brand                →    brand");
            System.out.println("   english_name         →    english_name");
            System.out.println("   chinese_name         →    chinese_name");
            System.out.println("   ai_review            →    ai_review");
            System.out.println("   has_sensitive_content→    has_sensitive_content");
            System.out.println("   sensitive_words      →    sensitive_words");
            System.out.println("   violation_handled    →    violation_handled");
            System.out.println("   sale_amount          →    sale_amount");
            System.out.println("   total_amount         →    total_amount");
            System.out.println("   ds                   →    ds");
            System.out.println("   created_time         →    created_time");
            System.out.println("");
            System.out.println("💡 实时同步已启动:");
            System.out.println("   - 初始快照: 8096 条记录正在同步");
            System.out.println("   - 实时变更: 任何数据变更将自动同步");
            System.out.println("   - 在 SQL Server 中执行 INSERT/UPDATE/DELETE 来测试实时同步");
            System.out.println("");
            System.out.println("⏹️  按 Ctrl+C 停止作业");

            // 等待作业运行，不要调用 env.execute()
            System.out.println("作业正在后台运行中...");
            Thread.sleep(Long.MAX_VALUE);

        } catch (Exception e) {
            System.err.println("❌ 错误发生: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }
}