

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Kafka_Flink_sql3 {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // 创建源表（保持不变）
        tenv.executeSql(
                "CREATE TABLE t_kafka_sales_source (\n" +
                        "  `id` STRING,\n" +
                        "  `order_id` STRING,\n" +
                        "  `user_id` STRING,\n" +
                        "  `user_name` STRING,\n" +
                        "  `product_id` STRING,\n" +
                        "  `size` STRING,\n" +
                        "  `item_id` STRING,\n" +
                        "  `sale_amount` DECIMAL(10,2),\n" +
                        "  `sale_num` BIGINT,\n" +
                        "  `total_amount` DECIMAL(10,2),\n" +
                        "  `product_name` STRING,\n" +
                        "  `ds` STRING,\n" +
                        "  `ts` STRING,\n" +
                        "  `insert_time` BIGINT,\n" +
                        "  `op` STRING,\n" +
                        "  `cdc_timestamp` BIGINT,\n" +
                        "  `event_time` AS \n" +
                        "    CASE \n" +
                        "      WHEN ts IS NOT NULL AND ts <> '' THEN\n" +
                        "        CASE \n" +
                        "          WHEN CHAR_LENGTH(REGEXP_REPLACE(ts, '\\.\\d+', '')) <= 10 \n" +
                        "          THEN TO_TIMESTAMP_LTZ(CAST(CAST(ts AS DOUBLE) * 1000 AS BIGINT), 3)\n" +
                        "          ELSE TO_TIMESTAMP_LTZ(CAST(CAST(ts AS DOUBLE) AS BIGINT), 3)\n" +
                        "        END\n" +
                        "      ELSE TO_TIMESTAMP_LTZ(insert_time, 3)\n" +
                        "    END,\n" +
                        "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'realtime_v3_order_info',\n" +
                        "  'properties.bootstrap.servers' = '172.17.55.4:9092',\n" +
                        "  'properties.group.id' = 'flink-sales-agg-" + System.currentTimeMillis() + "',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json',\n" +
                        "  'json.ignore-parse-errors' = 'true'\n" +
                        ")"
        );

        // 2. 创建日期过滤的视图，只保留2025-10-23当天的数据
        tenv.executeSql(
                "CREATE TEMPORARY VIEW t_daily_sales AS\n" +
                        "SELECT \n" +
                        "  id, order_id, user_name,product_id,sale_num, total_amount, event_time, op\n" +
                        "FROM t_kafka_sales_source\n" +
                        "WHERE \n" +
                        "  op IN ('r', '+I', '+U')\n" +
                        "  AND CAST(event_time AS DATE) = DATE '2025-10-23'"
        );

        // 3. 每10分钟窗口聚合（仅23号数据）- 去掉ORDER BY
//        System.out.println("=== 2025-10-23 每10分钟销售统计 ===");
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
                        "    WHERE DATE_FORMAT(event_time, 'yyyy-MM-dd') = '2025-10-23'\n" +
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
                        "        WHERE DATE_FORMAT(event_time, 'yyyy-MM-dd') = '2025-10-23'\n" +
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
                        "        WHERE DATE_FORMAT(event_time, 'yyyy-MM-dd') = '2025-10-23'\n" +
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