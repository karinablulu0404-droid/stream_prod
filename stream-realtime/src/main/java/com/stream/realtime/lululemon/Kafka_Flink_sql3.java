package com.stream.realtime.lululemon;



import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Kafka_Flink_sql3 {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");

        // 禁用 Hadoop 原生库加载
        System.setProperty("hadoop.home.dir", "C:\\"); // 设置为一个存在的目录
        System.setProperty("hadoop.tmp.dir", System.getProperty("java.io.tmpdir"));

        // 禁用 HBase delegation token provider
        System.setProperty("security.delegation.token.provider.classes", "");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        //todo: after这种嵌套格式 是 Debezium 的标准输出格式。其实不是你写出来的“奇怪形式”，而是 Flink CDC（或 Debezium）Kafka 数据的 JSON 结构决定的。
        //todo: 也就是说：🧠 这是由上游数据结构引起的，不是 Flink 自己生成的。

        tenv.executeSql(
                "CREATE TABLE t_kafka_sales_source (\n" +
                        "  `before` ROW<id INT>,\n" +
                        "  `after` ROW<\n" +
                        "    id INT,\n" +
                        "    order_id STRING,\n" +
                        "    user_id STRING,\n" +
                        "    user_name STRING,\n" +
                        "    product_id STRING,\n" +
                        "    size STRING,\n" +
                        "    item_id STRING,\n" +
                        "    sale_amount DECIMAL(10,2),\n" +
                        "    sale_num BIGINT,\n" +
                        "    total_amount DECIMAL(10,2),\n" +
                        "    product_name STRING,\n" +
                        "    ds STRING,\n" +
                        "    ts STRING,\n" +
                        "    insert_time BIGINT\n" +
                        "  >,\n" +
                        "  `op` STRING,\n" +
                        "  `source` MAP<STRING, STRING>,\n" +
                        "  `ts_ms` BIGINT,\n" +
                        "  `transaction` MAP<STRING, STRING>,\n" +
                        "  `event_time` AS (\n" +
                        "     CASE \n" +
                        "       WHEN `after`.`insert_time` IS NOT NULL THEN TO_TIMESTAMP_LTZ(`after`.`insert_time`, 3)\n" +
                        "       ELSE CURRENT_TIMESTAMP\n" +
                        "     END\n" +
                        "  ),\n" +
                        "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'realtime_v3_oms_order_dtl2',\n" +
                        "  'properties.bootstrap.servers' = '192.168.200.101:9092',\n" +
                        "  'properties.group.id' = 'flink-sales-agg-" + System.currentTimeMillis() + "',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json',\n" +
                        "  'json.ignore-parse-errors' = 'true',\n" +
                        "  'json.fail-on-missing-field' = 'false'\n" +
                        ")"
        );

        // 第二步：创建扁平化视图
        tenv.executeSql(
                "CREATE VIEW t_kafka_flat AS\n" +
                        "SELECT \n" +
                        "  after.id AS id,\n" +
                        "  after.order_id AS order_id,\n" +
                        "  after.user_id AS user_id,\n" +
                        "  after.user_name AS user_name,\n" +
                        "  after.product_id AS product_id,\n" +
                        "  after.size AS size,\n" +
                        "  after.item_id AS item_id,\n" +
                        "  after.sale_amount AS sale_amount,\n" +
                        "  after.sale_num AS sale_num,\n" +
                        "  after.total_amount AS total_amount,\n" +
                        "  after.product_name AS product_name,\n" +
                        "  after.ds AS ds,\n" +
                        "  after.ts AS ts,\n" +
                        "  after.insert_time AS insert_time,\n" +
                        "  op,\n" +
                        "  ts_ms\n" +
                        "FROM t_kafka_sales_source"
        );

// 第三步：查询
        tenv.executeSql("SELECT * FROM t_kafka_flat").print();





        tenv.executeSql(
                "SELECT * FROM t_kafka_sales_source "
        ).print();

        // 2. 创建日期过滤的视图，只保留2025-10-25当天的数据
        tenv.executeSql(
                "CREATE TEMPORARY VIEW t_daily_sales AS\n" +
                        "SELECT \n" +
                        "  id, order_id, user_name,product_id,sale_num, total_amount, event_time, op\n" +
                        "FROM t_kafka_sales_source\n" +
                        "WHERE \n" +
                        "  op IN ('r', '+I', '+U')\n" +
                        "  AND CAST(event_time AS DATE) = DATE '2025-10-25'"
        );

        // 3. 每10分钟窗口聚合（仅25号数据）- 去掉ORDER BY
//        System.out.println("=== 2025-10-25 每10分钟销售统计 ===");
//        tenv.executeSql(
//                "SELECT\n" +
//                        "  TUMBLE_START(event_time, INTERVAL '10' MINUTE) AS window_start,\n" +
//                        "  TUMBLE_END(event_time, INTERVAL '10' MINUTE) AS window_end,\n" +
//                        "  SUM(total_amount) AS window_sales,\n" +
//                        "  COUNT(DISTINCT order_id) AS order_count\n" +
//                        "FROM t_daily_sales\n" +
//                        "GROUP BY TUMBLE(event_time, INTERVAL '10' MINUTE)"
//        ).print();




        tenv.executeSql(
                "WITH window_gmv AS (\n" +
                        "    SELECT \n" +
                        "        window_start,\n" +
                        "        window_end,\n" +
                        "        SUM(CAST(total_amount AS DECIMAL(18,2))) as total_gmv\n" +
                        "    FROM TABLE(\n" +
                        "        CUMULATE(TABLE t_kafka_sales_source, DESCRIPTOR(event_time), INTERVAL '10' MINUTES, INTERVAL '1' DAY)\n" +
                        "    )\n" +
                        "    WHERE DATE_FORMAT(event_time, 'yyyy-MM-dd') = '2025-10-25'\n" +
                        "    GROUP BY window_start, window_end\n" +
                        "),\n" +
                        "top_ids AS (\n" +
                        "    SELECT \n" +
                        "        window_start,\n" +
                        "        window_end,\n" +
                        "        LISTAGG(id, ',') as top5_ids\n" +
                        "    FROM (\n" +
                        "        SELECT \n" +
                        "            window_start,\n" +
                        "            window_end,\n" +
                        "            id,\n" +
                        "            ROW_NUMBER() OVER (\n" +
                        "                PARTITION BY window_start, window_end \n" +
                        "                ORDER BY SUM(CAST(total_amount AS DECIMAL(18,2))) DESC\n" +
                        "            ) as rn\n" +
                        "        FROM TABLE(\n" +
                        "            CUMULATE(TABLE t_kafka_sales_source, DESCRIPTOR(event_time), INTERVAL '10' MINUTES, INTERVAL '1' DAY)\n" +
                        "        )\n" +
                        "        WHERE DATE_FORMAT(event_time, 'yyyy-MM-dd') = '2025-10-25'\n" +
                        "        GROUP BY window_start, window_end, id\n" +
                        "    )\n" +
                        "    WHERE rn <= 5\n" +
                        "    GROUP BY window_start, window_end\n" +
                        "),\n" +
                        "top_products AS (\n" +
                        "    SELECT \n" +
                        "        window_start,\n" +
                        "        window_end,\n" +
                        "        LISTAGG(product_id, ',') as top5_product_ids\n" +
                        "    FROM (\n" +
                        "        SELECT \n" +
                        "            window_start,\n" +
                        "            window_end,\n" +
                        "            product_id,\n" +
                        "            ROW_NUMBER() OVER (\n" +
                        "                PARTITION BY window_start, window_end \n" +
                        "                ORDER BY SUM(CAST(total_amount AS DECIMAL(18,2))) DESC\n" +
                        "            ) as rn\n" +
                        "        FROM TABLE(\n" +
                        "            CUMULATE(TABLE t_kafka_sales_source, DESCRIPTOR(event_time), INTERVAL '10' MINUTES, INTERVAL '1' DAY)\n" +
                        "        )\n" +
                        "        WHERE DATE_FORMAT(event_time, 'yyyy-MM-dd') = '2025-10-25'\n" +
                        "        GROUP BY window_start, window_end, product_id\n" +
                        "    )\n" +
                        "    WHERE rn <= 5\n" +
                        "    GROUP BY window_start, window_end\n" +
                        ")\n" +
                        "SELECT \n" +
                        "    DATE_FORMAT(wg.window_start, 'yyyy-MM-dd') as order_date, \n" +
                        "    wg.window_start, \n" +
                        "    wg.window_end, \n" +
                        "    wg.total_gmv as GMV,\n" +
                        "    COALESCE(ti.top5_ids, '') as top5_ids,\n" +
                        "    COALESCE(tp.top5_product_ids, '') as top5_product_ids\n" +
                        "FROM window_gmv wg\n" +
                        "LEFT JOIN top_ids ti ON wg.window_start = ti.window_start AND wg.window_end = ti.window_end\n" +
                        "LEFT JOIN top_products tp ON wg.window_start = tp.window_start AND wg.window_end = tp.window_end"
        ).print();















        env.execute("Kafka_Flink_sql");
    }
}
