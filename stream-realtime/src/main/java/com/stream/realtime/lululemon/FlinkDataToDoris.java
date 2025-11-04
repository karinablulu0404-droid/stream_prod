package com.stream.realtime.lululemon;

import com.stream.core.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.LocalDate;
import java.time.ZoneId;

public class FlinkDataToDoris {

    @SneakyThrows
    public static void main(String[] args) {
        // 系统属性设置
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("hadoop.home.dir", "C:\\");
        System.setProperty("hadoop.tmp.dir", System.getProperty("java.io.tmpdir"));
        System.setProperty("security.delegation.token.provider.classes", "");

        System.out.println("=== FlinkDataToDoris 启动 ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        StreamTableEnvironment sEnv = StreamTableEnvironment.create(env);
        sEnv.getConfig().getConfiguration().setString("table.local-time-zone","Asia/Shanghai");
        sEnv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout","30s");

        ZoneId sh = ZoneId.of("Asia/Shanghai");
        long today0Millis = LocalDate.now(ZoneId.of("Asia/Shanghai")).atStartOfDay(sh).toInstant().toEpochMilli();

        // Kafka 源表
        String ddl_kafka_oms_order_info = String.format(
                "CREATE TABLE IF NOT EXISTS kafka_oms_order_dtl (                           \n" +
                        "    id BIGINT,                                                                                                     \n" +
                        "    order_id STRING,                                                                                               \n" +
                        "    user_id STRING,                                                                                                \n" +
                        "    user_name STRING,                                                                                              \n" +
                        "    phone_number STRING,                                                                                           \n" +
                        "    product_link STRING,                                                                                           \n" +
                        "    product_id STRING,                                                                                             \n" +
                        "    color STRING,                                                                                                  \n" +
                        "    size STRING,                                                                                                   \n" +
                        "    item_id STRING,                                                                                                \n" +
                        "    material STRING,                                                                                               \n" +
                        "    sale_num STRING,                                                                                               \n" +
                        "    sale_amount STRING,                                                                                            \n" +
                        "    total_amount STRING,                                                                                           \n" +
                        "    product_name STRING,                                                                                           \n" +
                        "    is_online_sales STRING,                                                                                        \n" +
                        "    shipping_address STRING,                                                                                       \n" +
                        "    recommendations_product_ids STRING,                                                                           \n" +
                        "    ds STRING,                                                                                                    \n" +
                        "    ts BIGINT,                                                                                                     \n" +
                        "    ts_ms AS CASE WHEN ts < 100000000000 THEN TO_TIMESTAMP_LTZ(ts * 1000, 3) ELSE TO_TIMESTAMP_LTZ(ts, 3) END,     \n" +
                        "    insert_time STRING,                                                                                            \n" +
                        "    table_name STRING,                                                                                             \n" +
                        "    op STRING,                                                                                                     \n" +
                        "    proc_time AS PROCTIME(),                                                                                       \n" +
                        "    WATERMARK FOR ts_ms AS ts_ms - INTERVAL '5' SECOND                                                             \n" +
                        ") WITH (                                                                                                             \n" +
                        "    'connector' = 'kafka',                                                                                         \n" +
                        "    'topic' = 'realtime_v3_order_info',                                                                            \n" +
                        "    'properties.bootstrap.servers' = 'cdh01:9092,cdh02:9092,cdh03:9092',                                            \n" +
                        "    'properties.group.id' = 'order-analysis1',                                                                     \n" +
                        "    'scan.startup.mode' = 'timestamp',                                                                             \n" +
                        "    'scan.startup.timestamp-millis' = '%d',                                                                        \n" +
                        "    'format' = 'json',                                                                                             \n" +
                        "    'json.fail-on-missing-field' = 'false',                                                                        \n" +
                        "    'json.ignore-parse-errors' = 'true'                                                                            \n" +
                        ")", today0Millis);

        // Doris Sink 表 - 修正连接配置，使用正确的参数名
        // 修改 Doris Sink 配置，添加密码和更多连接参数
        String dorisSinkDDL = String.format(
                "CREATE TABLE IF NOT EXISTS report_lululemon_window_gmv_topN (                      \n" +
                        "    ds DATE,                                                                       \n" +
                        "    window_start STRING,                                                           \n" +
                        "    window_end STRING,                                                             \n" +
                        "    win_gmv DECIMAL(18,2),                                                         \n" +
                        "    win_gmv_ids STRING,                                                            \n" +
                        "    top5_product_ids STRING                                                        \n" +
                        ") WITH (                                                                           \n" +
                        "    'connector' = 'doris',                                                         \n" +
                        "    'fenodes' = '172.17.42.124:8030',                                             \n" +
                        "    'table.identifier' = 'realtimereport.report_lululemon_window_gmv_topN',        \n" +
                        "    'username' = 'root',                                                           \n" +
                        "    'password' = '',                                             \n" +  // 添加实际密码
                        "    'sink.buffer-flush.max-rows' = '1000',                                        \n" +
                        "    'sink.max-retries' = '3',                                                      \n" +
                        "    'sink.buffer-flush.interval' = '1s',                                           \n" +
                        "    'sink.enable-2pc' = 'false',                                                   \n" +
                        "    'sink.label-prefix' = 'doris_sink_%d'                                          \n" +
                        ")", System.currentTimeMillis());

        try {
            System.out.println("步骤 1/3: 创建 Kafka 源表...");
            sEnv.executeSql(ddl_kafka_oms_order_info);
            System.out.println("✓ Kafka 表创建成功");

            System.out.println("步骤 2/3: 创建 Doris Sink 表...");
            sEnv.executeSql(dorisSinkDDL);
            System.out.println("✓ Doris Sink 表创建成功");

            System.out.println("步骤 3/3: 执行数据插入...");

            // 使用更稳定的插入语句
            String insertSQL =
                    "INSERT INTO report_lululemon_window_gmv_topN " +
                            "SELECT " +
                            "  CAST(DATE_FORMAT(LOCALTIMESTAMP, 'yyyy-MM-dd') AS DATE) AS ds, " +
                            "  DATE_FORMAT(LOCALTIMESTAMP, 'yyyy-MM-dd HH:mm:ss') AS window_start, " +
                            "  DATE_FORMAT(LOCALTIMESTAMP, 'yyyy-MM-dd HH:mm:ss') AS window_end, " +
                            "  CAST(1000.00 AS DECIMAL(18,2)) AS win_gmv, " +
                            "  'test_ids' AS win_gmv_ids, " +
                            "  'test_top5' AS top5_product_ids " +
                            "FROM kafka_oms_order_dtl " +
                            "WHERE id IS NOT NULL " +  // 添加过滤条件避免空数据
                            "LIMIT 1";

            sEnv.executeSql(insertSQL);
            System.out.println("✓ 插入语句执行成功");

            System.out.println("开始执行 Flink 作业...");
            env.execute("FlinkDataToDoris");

        } catch (Exception e) {
            System.err.println("❌ 程序执行失败:");
            e.printStackTrace();
            System.err.println("\n排查建议:");
            System.err.println("1. 检查 Doris 服务状态: ps aux | grep doris");
            System.err.println("2. 检查端口监听: netstat -tlnp | grep 8030");
            System.err.println("3. 测试连接: telnet 172.17.42.124 8030");
            System.err.println("4. 验证表是否存在: 登录 Doris 执行 SHOW TABLES FROM realtimereport");
            System.err.println("5. 检查 Kafka 服务状态和主题是否存在");
            System.err.println("6. 检查防火墙设置");
            System.err.println("7. 确认 Doris FE 服务正在运行");
            System.err.println("8. 检查 Flink 连接器依赖是否正确");
            System.err.println("9. 检查 Doris 连接器版本，确保参数名称正确");
            System.exit(1);
        }
    }
}