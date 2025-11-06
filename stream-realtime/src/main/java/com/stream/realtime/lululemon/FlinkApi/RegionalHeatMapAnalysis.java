package com.stream.realtime.lululemon.FlinkApi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.math.BigDecimal;

public class RegionalHeatMapAnalysis {

    // Doris 连接配置
    private static final String DORIS_FE_NODES = "172.17.42.124:8030";
    private static final String DATABASE = "test";
    private static final String TABLE_NAME = "user_access_log";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "Wjk19990921.";

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        System.out.println("=== 开始连接 Doris ===");

        try {
            // 方案1：跳过时间字段，使用 doris.read.field 排除 access_time
            String createTableSql = String.format(
                    "CREATE TABLE user_access_log (\n" +
                            "  id BIGINT,\n" +
                            "  user_id STRING,\n" +
                            "  region STRING,\n" +
                            "  username STRING,\n" +
                            "  city STRING,\n" +
                            "  access_type STRING,\n" +
                            "  page_url STRING,\n" +
                            "  stay_duration INT,\n" +
                            "  device_type STRING,\n" +
                            "  ip_address STRING\n" +
                            ") WITH (\n" +
                            "  'connector' = 'doris',\n" +
                            "  'fenodes' = '%s',\n" +
                            "  'table.identifier' = '%s.%s',\n" +
                            "  'username' = '%s',\n" +
                            "  'password' = '%s',\n" +
                            "  'doris.read.field' = 'id,user_id,region,username,city,access_type,page_url,stay_duration,device_type,ip_address',\n" + // 跳过 access_time
                            "  'doris.request.retries' = '5',\n" +
                            "  'doris.request.connect.timeout.ms' = '30000',\n" +
                            "  'doris.request.read.timeout.ms' = '30000',\n" +
                            "  'doris.request.query.timeout.s' = '3600',\n" +
                            "  'doris.request.tablet.size' = '1000'\n" +
                            ")", DORIS_FE_NODES, DATABASE, TABLE_NAME, USERNAME, PASSWORD);

            System.out.println("执行创建表SQL...");
            tableEnv.executeSql(createTableSql);
            System.out.println("表连接创建成功");

            // 先执行简单测试查询（不包含时间字段）
            System.out.println("执行测试查询...");
            String testSql = "SELECT COUNT(*) as total_count FROM user_access_log";
            Table testTable = tableEnv.sqlQuery(testSql);

            DataStream<Row> testStream = tableEnv.toChangelogStream(testTable);
            testStream.map(new MapFunction<Row, String>() {
                @Override
                public String map(Row row) throws Exception {
                    Long count = (Long) row.getField("total_count");
                    return "测试查询成功！总记录数: " + count;
                }
            }).print();

            // 执行简化版热力分析查询
            System.out.println("执行简化版热力分析查询...");
            String simpleHeatMapSql =
                    "SELECT \n" +
                            "  region,\n" +
                            "  COUNT(*) as access_count,\n" +
                            "  COUNT(DISTINCT user_id) as unique_users,\n" +
                            "  ROUND(COUNT(*) * 0.7 + COUNT(DISTINCT user_id) * 0.3, 2) as heat_value,\n" +
                            "  CASE \n" +
                            "    WHEN COUNT(*) >= 10 THEN '爆热'\n" +
                            "    WHEN COUNT(*) >= 5 THEN '高热'\n" +
                            "    WHEN COUNT(*) >= 3 THEN '中热'\n" +
                            "    WHEN COUNT(*) >= 1 THEN '低热'\n" +
                            "    ELSE '常温'\n" +
                            "  END as heat_level\n" +
                            "FROM user_access_log\n" +
                            "GROUP BY region";

            Table resultTable = tableEnv.sqlQuery(simpleHeatMapSql);

            // 使用toChangelogStream来处理更新消息
            DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);

            // 输出热力分析结果 - 修复类型转换问题
            resultStream.map(new MapFunction<Row, String>() {
                @Override
                public String map(Row row) throws Exception {
                    String region = String.valueOf(row.getField("region"));
                    Long accessCount = (Long) row.getField("access_count");
                    Long uniqueUsers = (Long) row.getField("unique_users");

                    // 修复：正确处理 BigDecimal 到 Double 的转换
                    Object heatValueObj = row.getField("heat_value");
                    Double heatValue;
                    if (heatValueObj instanceof BigDecimal) {
                        heatValue = ((BigDecimal) heatValueObj).doubleValue();
                    } else if (heatValueObj instanceof Double) {
                        heatValue = (Double) heatValueObj;
                    } else {
                        heatValue = 0.0;
                        System.err.println("警告: heat_value 字段类型未知: " + heatValueObj.getClass().getName());
                    }

                    String heatLevel = String.valueOf(row.getField("heat_level"));

                    // 添加行类型信息
                    String rowKind = row.getKind().shortString();

                    return String.format("[%s] 区域: %-4s | 访问量: %2d | 独立用户: %2d | 热力值: %5.2f | 热力等级: %s",
                            rowKind, region, accessCount, uniqueUsers, heatValue, heatLevel);
                }
            }).print();

            // 执行任务
            env.execute("Regional Heat Map Analysis");

        } catch (Exception e) {
            System.err.println("=== 执行失败 ===");
            System.err.println("错误类型: " + e.getClass().getName());
            System.err.println("错误信息: " + e.getMessage());
            e.printStackTrace();

            // 如果还是失败，尝试使用备选方案
            if (e.getMessage().contains("DATETIME") || e.getMessage().contains("TIMESTAMP") || e.getMessage().contains("ClassCastException")) {
                System.err.println("\n=== 尝试备选方案：使用更安全的类型处理 ===");
                trySafeTypeApproach(env, tableEnv);
            }
        }
    }

    // 备选方案：使用更安全的类型处理
    private static void trySafeTypeApproach(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        try {
            System.out.println("=== 尝试使用更安全的类型处理方案 ===");

            String createTableSql = String.format(
                    "CREATE TABLE user_access_log_safe (\n" +
                            "  id BIGINT,\n" +
                            "  user_id STRING,\n" +
                            "  region STRING,\n" +
                            "  username STRING,\n" +
                            "  city STRING,\n" +
                            "  access_type STRING,\n" +
                            "  page_url STRING,\n" +
                            "  stay_duration INT,\n" +
                            "  device_type STRING,\n" +
                            "  ip_address STRING\n" +
                            ") WITH (\n" +
                            "  'connector' = 'doris',\n" +
                            "  'fenodes' = '%s',\n" +
                            "  'table.identifier' = '%s.%s',\n" +
                            "  'username' = '%s',\n" +
                            "  'password' = '%s',\n" +
                            "  'doris.read.field' = 'id,user_id,region,username,city,access_type,page_url,stay_duration,device_type,ip_address',\n" +
                            "  'doris.request.retries' = '5',\n" +
                            "  'doris.request.connect.timeout.ms' = '30000',\n" +
                            "  'doris.request.read.timeout.ms' = '30000',\n" +
                            "  'doris.request.query.timeout.s' = '3600',\n" +
                            "  'doris.request.tablet.size' = '1000'\n" +
                            ")", DORIS_FE_NODES, DATABASE, TABLE_NAME, USERNAME, PASSWORD);

            tableEnv.executeSql(createTableSql);
            System.out.println("安全类型方案表连接创建成功");

            // 使用 CAST 确保类型一致性
            String safeHeatMapSql =
                    "SELECT \n" +
                            "  region,\n" +
                            "  COUNT(*) as access_count,\n" +
                            "  COUNT(DISTINCT user_id) as unique_users,\n" +
                            "  CAST(ROUND(COUNT(*) * 0.7 + COUNT(DISTINCT user_id) * 0.3, 2) AS DOUBLE) as heat_value,\n" + // 使用 CAST 确保类型
                            "  CASE \n" +
                            "    WHEN COUNT(*) >= 10 THEN '爆热'\n" +
                            "    WHEN COUNT(*) >= 5 THEN '高热'\n" +
                            "    WHEN COUNT(*) >= 3 THEN '中热'\n" +
                            "    WHEN COUNT(*) >= 1 THEN '低热'\n" +
                            "    ELSE '常温'\n" +
                            "  END as heat_level\n" +
                            "FROM user_access_log_safe\n" +
                            "GROUP BY region";

            Table resultTable = tableEnv.sqlQuery(safeHeatMapSql);
            DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);

            // 使用安全的类型转换
            resultStream.map(new MapFunction<Row, String>() {
                @Override
                public String map(Row row) throws Exception {
                    try {
                        String region = safeStringValue(row.getField("region"));
                        Long accessCount = safeLongValue(row.getField("access_count"));
                        Long uniqueUsers = safeLongValue(row.getField("unique_users"));
                        Double heatValue = safeDoubleValue(row.getField("heat_value"));
                        String heatLevel = safeStringValue(row.getField("heat_level"));

                        String rowKind = row.getKind().shortString();

                        return String.format("[%s] 区域: %-4s | 访问量: %2d | 独立用户: %2d | 热力值: %5.2f | 热力等级: %s",
                                rowKind, region, accessCount, uniqueUsers, heatValue, heatLevel);
                    } catch (Exception e) {
                        return "数据处理错误: " + e.getMessage();
                    }
                }
            }).print();

            env.execute("Regional Heat Map Analysis - Safe Type Approach");

        } catch (Exception e2) {
            System.err.println("=== 安全类型方案也失败 ===");
            System.err.println("错误信息: " + e2.getMessage());
            e2.printStackTrace();

            // 最终建议
            System.err.println("\n=== 最终解决方案建议 ===");
            System.err.println("1. 检查字段类型映射，使用 CAST 在 SQL 中明确指定类型");
            System.err.println("2. 在代码中使用安全的类型转换方法");
            System.err.println("3. 考虑升级 Flink-Doris-Connector 版本");
        }
    }

    // 安全的类型转换方法
    private static String safeStringValue(Object value) {
        if (value == null) return "null";
        return String.valueOf(value);
    }

    private static Long safeLongValue(Object value) {
        if (value == null) return 0L;
        if (value instanceof Long) return (Long) value;
        if (value instanceof Integer) return ((Integer) value).longValue();
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private static Double safeDoubleValue(Object value) {
        if (value == null) return 0.0;
        if (value instanceof Double) return (Double) value;
        if (value instanceof BigDecimal) return ((BigDecimal) value).doubleValue();
        if (value instanceof Float) return ((Float) value).doubleValue();
        if (value instanceof Long) return ((Long) value).doubleValue();
        if (value instanceof Integer) return ((Integer) value).doubleValue();
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}