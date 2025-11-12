package com.stream.realtime.lululemon.FlinkApi.Plinglun;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class ProductNameParser {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);

        System.out.println("🚀 开始连接 Doris 数据库...");

        // 创建 Doris 源表连接
        String sourceTableSql = "CREATE TABLE doris_source (\n" +
                "  order_id STRING,\n" +
                "  user_id STRING,\n" +
                "  user_name STRING,\n" +
                "  product_id STRING,\n" +
                "  product_name STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '172.17.42.124:8030',\n" +
                "  'table.identifier' = 'test.order_table',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'Wjk19990921.',\n" +
                "  'doris.batch.size' = '1000',\n" +
                "  'sink.max-retries' = '3'\n" +
                ")";

        tableEnv.executeSql(sourceTableSql);

        // 使用 SQL 解析 product_name 字段
        String analysisSql = "SELECT \n" +
                "  order_id,\n" +
                "  product_id,\n" +
                "  product_name as original_name,\n" +
                "  \n" +
                "  -- 提取品牌 (使用 Flink SQL 支持的函数)\n" +
                "  CASE \n" +
                "    WHEN product_name LIKE '%lululemon%' THEN 'lululemon'\n" +
                "    WHEN POSITION('丨' IN product_name) > 0 THEN SUBSTRING(product_name, 1, POSITION('丨' IN product_name) - 1)\n" +
                "    ELSE '其他品牌'\n" +
                "  END as brand,\n" +
                "  \n" +
                "  -- 提取产品系列\n" +
                "  CASE \n" +
                "    WHEN product_name LIKE '%Align%' OR product_name LIKE '%Align™%' THEN 'Align'\n" +
                "    WHEN product_name LIKE '%Define%' THEN 'Define'\n" +
                "    WHEN product_name LIKE '%Scuba%' THEN 'Scuba'\n" +
                "    WHEN product_name LIKE '%Wunder Train%' THEN 'Wunder Train'\n" +
                "    WHEN product_name LIKE '%Fast and Free%' THEN 'Fast and Free'\n" +
                "    WHEN product_name LIKE '%Energy%' THEN 'Energy'\n" +
                "    WHEN product_name LIKE '%Like a Cloud%' THEN 'Like a Cloud'\n" +
                "    WHEN product_name LIKE '%Swiftly%' THEN 'Swiftly'\n" +
                "    WHEN product_name LIKE '%ABC%' THEN 'ABC'\n" +
                "    WHEN product_name LIKE '%Commission%' THEN 'Commission'\n" +
                "    WHEN product_name LIKE '%Metal Vent%' THEN 'Metal Vent'\n" +
                "    WHEN product_name LIKE '%Pace Breaker%' THEN 'Pace Breaker'\n" +
                "    ELSE '其他系列'\n" +
                "  END as product_series,\n" +
                "  \n" +
                "  -- 提取性别\n" +
                "  CASE \n" +
                "    WHEN product_name LIKE '%女士%' THEN '女士'\n" +
                "    WHEN product_name LIKE '%男士%' THEN '男士'\n" +
                "    ELSE '通用'\n" +
                "  END as gender,\n" +
                "  \n" +
                "  -- 提取产品类型\n" +
                "  CASE \n" +
                "    WHEN product_name LIKE '%背心%' THEN '背心'\n" +
                "    WHEN product_name LIKE '%紧身裤%' THEN '紧身裤'\n" +
                "    WHEN product_name LIKE '%夹克%' THEN '夹克'\n" +
                "    WHEN product_name LIKE '%运动内衣%' THEN '运动内衣'\n" +
                "    WHEN product_name LIKE '%T恤%' OR product_name LIKE '%T 恤%' THEN 'T恤'\n" +
                "    WHEN product_name LIKE '%短裤%' THEN '短裤'\n" +
                "    WHEN product_name LIKE '%长裤%' THEN '长裤'\n" +
                "    WHEN product_name LIKE '%连帽衫%' THEN '连帽衫'\n" +
                "    WHEN product_name LIKE '%卫衣%' THEN '卫衣'\n" +
                "    WHEN product_name LIKE '%袜子%' THEN '袜子'\n" +
                "    WHEN product_name LIKE '%包%' OR product_name LIKE '%背包%' THEN '包袋'\n" +
                "    WHEN product_name LIKE '%水瓶%' OR product_name LIKE '%水杯%' THEN '水瓶'\n" +
                "    WHEN product_name LIKE '%裙%' THEN '裙'\n" +
                "    WHEN product_name LIKE '%鞋%' THEN '鞋'\n" +
                "    WHEN product_name LIKE '%帽%' THEN '帽'\n" +
                "    WHEN product_name LIKE '%瑜伽垫%' THEN '瑜伽垫'\n" +
                "    ELSE '其他'\n" +
                "  END as product_type,\n" +
                "  \n" +
                "  -- 提取颜色\n" +
                "  CASE \n" +
                "    WHEN product_name LIKE '%白色%' THEN '白色'\n" +
                "    WHEN product_name LIKE '%黑色%' THEN '黑色'\n" +
                "    WHEN product_name LIKE '%红色%' THEN '红色'\n" +
                "    WHEN product_name LIKE '%海军蓝%' THEN '海军蓝'\n" +
                "    WHEN product_name LIKE '%石墨灰%' THEN '石墨灰'\n" +
                "    WHEN product_name LIKE '%骨白%' THEN '骨白'\n" +
                "    WHEN product_name LIKE '%犀牛灰%' THEN '犀牛灰'\n" +
                "    WHEN product_name LIKE '%蒸汽灰%' THEN '蒸汽灰'\n" +
                "    WHEN product_name LIKE '%浅象牙白%' THEN '浅象牙白'\n" +
                "    WHEN product_name LIKE '%暮光玫瑰棕%' THEN '暮光玫瑰棕'\n" +
                "    WHEN product_name LIKE '%橡木棕%' THEN '橡木棕'\n" +
                "    WHEN product_name LIKE '%蔷薇粉%' THEN '蔷薇粉'\n" +
                "    WHEN product_name LIKE '%薰衣草紫%' THEN '薰衣草紫'\n" +
                "    WHEN product_name LIKE '%褐粉%' THEN '褐粉'\n" +
                "    WHEN product_name LIKE '%绿洲灰%' THEN '绿洲灰'\n" +
                "    WHEN product_name LIKE '%熔岩棕%' THEN '熔岩棕'\n" +
                "    WHEN product_name LIKE '%棕榈绿%' THEN '棕榈绿'\n" +
                "    WHEN product_name LIKE '%太阳灰%' THEN '太阳灰'\n" +
                "    WHEN product_name LIKE '%水滴银%' THEN '水滴银'\n" +
                "    WHEN product_name LIKE '%杂色中灰%' THEN '杂色中灰'\n" +
                "    ELSE '未知颜色'\n" +
                "  END as color,\n" +
                "  \n" +
                "  -- 提取尺码\n" +
                "  CASE \n" +
                "    WHEN product_name LIKE '%XXS%' THEN 'XXS'\n" +
                "    WHEN product_name LIKE '%XS%' THEN 'XS'\n" +
                "    WHEN product_name LIKE '%S%' AND product_name NOT LIKE '%XS%' THEN 'S'\n" +
                "    WHEN product_name LIKE '%M%' THEN 'M'\n" +
                "    WHEN product_name LIKE '%L%' THEN 'L'\n" +
                "    WHEN product_name LIKE '%XL%' THEN 'XL'\n" +
                "    WHEN product_name LIKE '%XXL%' THEN 'XXL'\n" +
                "    WHEN product_name LIKE '%XXXL%' THEN 'XXXL'\n" +
                "    WHEN product_name LIKE '%均码%' THEN '均码'\n" +
                "    WHEN product_name LIKE '%O/S%' THEN 'O/S'\n" +
                "    ELSE '未知尺码'\n" +
                "  END as size,\n" +
                "  \n" +
                "  -- 提取产品型号\n" +
                "  CASE \n" +
                "    WHEN product_name LIKE '%LW1BS%' THEN 'LW1BS'\n" +
                "    WHEN product_name LIKE '%LW2C%' THEN 'LW2C'\n" +
                "    WHEN product_name LIKE '%LW3%' THEN 'LW3'\n" +
                "    WHEN product_name LIKE '%LM1%' THEN 'LM1'\n" +
                "    WHEN product_name LIKE '%LM2%' THEN 'LM2'\n" +
                "    ELSE '未知型号'\n" +
                "  END as product_model\n" +
                "  \n" +
                "FROM doris_source\n" +
                "WHERE product_name IS NOT NULL AND product_name <> ''";

        Table resultTable = tableEnv.sqlQuery(analysisSql);

        System.out.println("🎯 开始解析 product_name 字段...");
        System.out.println("==================================================");

        // 创建自定义输出格式 - 主要结果
        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);

        resultStream.map(row -> {
                    StringBuilder sb = new StringBuilder();
                    sb.append("\n📦 产品解析结果:\n");
                    sb.append("──────────────────────────────────────────────────\n");
                    sb.append("🆔 订单ID: ").append(row.getField(0)).append("\n");
                    sb.append("📦 产品ID: ").append(row.getField(1)).append("\n");
                    sb.append("📝 原始名称: ").append(row.getField(2)).append("\n");
                    sb.append("🏷️  品牌: ").append(row.getField(3)).append("\n");
                    sb.append("📚 产品系列: ").append(row.getField(4)).append("\n");
                    sb.append("👫 性别: ").append(row.getField(5)).append("\n");
                    sb.append("👕 产品类型: ").append(row.getField(6)).append("\n");
                    sb.append("🎨 颜色: ").append(row.getField(7)).append("\n");
                    sb.append("📏 尺码: ").append(row.getField(8)).append("\n");
                    sb.append("🔧 产品型号: ").append(row.getField(9)).append("\n");
                    sb.append("──────────────────────────────────────────────────\n");
                    return sb.toString();
                }, Types.STRING)
                .print();

        System.out.println("⏳ 开始执行 Flink 作业...");

        // 使用 collect() 方法获取统计信息（避免聚合问题）
        System.out.println("\n📊 正在统计数据量...");

        // 方法1：使用 tableEnv.executeSql() 并收集结果
        try {
            Table countTable = tableEnv.sqlQuery(
                    "SELECT COUNT(*) as total_count FROM doris_source WHERE product_name IS NOT NULL AND product_name <> ''"
            );

            // 使用临时表的方式输出统计信息
            String tempSinkSql = "CREATE TABLE temp_count_sink (\n" +
                    "  total_count BIGINT\n" +
                    ") WITH (\n" +
                    "  'connector' = 'print'\n" +
                    ")";
            tableEnv.executeSql(tempSinkSql);

            // 执行统计查询
            countTable.executeInsert("temp_count_sink");

        } catch (Exception e) {
            System.out.println("⚠️  统计信息获取失败，但主要解析过程将继续...");
        }

        env.execute("Doris Product Name Parser");

        // 作业完成后显示完成信息
        System.out.println("\n🎉 Flink 作业执行完成！");
        System.out.println("==================================================");
    }
}