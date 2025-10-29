import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.lang.String;
import java.lang.Exception;

public class Kafka_Flink_sql3 {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 添加 Flink 配置
        Configuration config = new Configuration();
        config.setInteger("rest.port", 8081);
        env.setParallelism(1);

        // 启用检查点（对于生产环境很重要）
        env.enableCheckpointing(10000); // 10秒

        // 创建源表 - 优化版本
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
                        "      ELSE TO_TIMESTAMP_LTZ(COALESCE(insert_time, cdc_timestamp), 3)\n" +
                        "    END,\n" +
                        "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'realtime_v3_order_info',\n" +
                        "  'properties.bootstrap.servers' = '172.17.55.4:9092',\n" +
                        "  'properties.group.id' = 'flink-sales-agg-" + System.currentTimeMillis() + "',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json',\n" +
                        "  'json.ignore-parse-errors' = 'true',\n" +
                        "  'json.timestamp-format.standard' = 'ISO-8601'\n" +
                        ")"
        );

        // 创建过滤视图 - 添加数据质量检查
        tenv.executeSql(
                "CREATE TEMPORARY VIEW t_cleaned_sales AS\n" +
                        "SELECT \n" +
                        "  id, order_id, user_name, product_id, sale_num, \n" +
                        "  CAST(total_amount AS DECIMAL(18,2)) as total_amount,\n" +
                        "  event_time, op\n" +
                        "FROM t_kafka_sales_source\n" +
                        "WHERE \n" +
                        "  op IN ('r', '+I', '+U')\n" +
                        "  AND total_amount IS NOT NULL\n" +
                        "  AND total_amount > 0\n" +
                        "  AND event_time IS NOT NULL"
        );

        // 执行主查询
        System.out.println("=== 开始执行销售数据分析 ===");

        TableResult result = tenv.executeSql(
                "WITH window_gmv AS (\n" +
                        "    SELECT \n" +
                        "        window_start,\n" +
                        "        window_end,\n" +
                        "        SUM(total_amount) as total_gmv\n" +
                        "    FROM TABLE(\n" +
                        "        CUMULATE(\n" +
                        "            TABLE t_cleaned_sales, \n" +
                        "            DESCRIPTOR(event_time), \n" +
                        "            INTERVAL '10' MINUTES, \n" +
                        "            INTERVAL '1' DAY\n" +
                        "        )\n" +
                        "    )\n" +
                        "    WHERE DATE_FORMAT(event_time, 'yyyy-MM-dd') = '2025-10-23'\n" +
                        "    GROUP BY window_start, window_end\n" +
                        "),\n" +
                        "top_ids AS (\n" +
                        "    SELECT \n" +
                        "        window_start,\n" +
                        "        window_end,\n" +
                        "        LISTAGG(id, ',') WITHIN GROUP (ORDER BY id) as top5_ids\n" +
                        "    FROM (\n" +
                        "        SELECT \n" +
                        "            window_start,\n" +
                        "            window_end,\n" +
                        "            id,\n" +
                        "            ROW_NUMBER() OVER (\n" +
                        "                PARTITION BY window_start, window_end \n" +
                        "                ORDER BY SUM(total_amount) DESC\n" +
                        "            ) as rn\n" +
                        "        FROM TABLE(\n" +
                        "            CUMULATE(\n" +
                        "                TABLE t_cleaned_sales, \n" +
                        "                DESCRIPTOR(event_time), \n" +
                        "                INTERVAL '10' MINUTES, \n" +
                        "                INTERVAL '1' DAY\n" +
                        "            )\n" +
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
                        "        LISTAGG(product_id, ',') WITHIN GROUP (ORDER BY product_id) as top5_product_ids\n" +
                        "    FROM (\n" +
                        "        SELECT \n" +
                        "            window_start,\n" +
                        "            window_end,\n" +
                        "            product_id,\n" +
                        "            ROW_NUMBER() OVER (\n" +
                        "                PARTITION BY window_start, window_end \n" +
                        "                ORDER BY SUM(total_amount) DESC\n" +
                        "            ) as rn\n" +
                        "        FROM TABLE(\n" +
                        "            CUMULATE(\n" +
                        "                TABLE t_cleaned_sales, \n" +
                        "                DESCRIPTOR(event_time), \n" +
                        "                INTERVAL '10' MINUTES, \n" +
                        "                INTERVAL '1' DAY\n" +
                        "            )\n" +
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
                        "LEFT JOIN top_products tp ON wg.window_start = tp.window_start AND wg.window_end = tp.window_end\n" +
                        "ORDER BY wg.window_start"
        );

        result.print();

        env.execute("Kafka_Flink_SQL_Analysis");
    }
}