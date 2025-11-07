package com.stream.realtime.lululemon.FlinkApi;

import com.alibaba.fastjson2.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Random;

public class FlinkApi_PathAnalysis {

    private static final Logger logger = LoggerFactory.getLogger(FlinkApi_PathAnalysis.class);

    private static final String TIME_ZONE = "Asia/Shanghai";
    private static final Random random = new Random();

    @SneakyThrows
    public static void main(String[] args) {
        logger.info("🚀 启动完整版路径分析作业 - 历史天+当天路径分析");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建持续运行的模拟数据流（秒级时间戳）
        DataStreamSource<String> source = (DataStreamSource<String>) env.addSource(new SecondTimestampUserBehaviorSource())
                .name("second-timestamp-data-source");

        // 解析JSON数据并处理秒级时间戳
        SingleOutputStreamOperator<JSONObject> parsedStream = source.flatMap(new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String s, Collector<JSONObject> collector) {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(s);
                            if (jsonObject != null && !jsonObject.isEmpty()) {
                                // 处理秒级时间戳
                                Long ts = jsonObject.getLong("ts");
                                if (ts != null) {
                                    // 秒级时间戳直接使用，不需要转换
                                    LocalDate localDate = Instant.ofEpochSecond(ts)
                                            .atZone(ZoneId.of(TIME_ZONE))
                                            .toLocalDate();

                                    jsonObject.put("log_date", localDate.toString());
                                    jsonObject.put("processed_ts", ts);

                                    collector.collect(jsonObject);
                                    logger.debug("📥 收到数据: {} [ts: {}] - {}",
                                            localDate, ts, jsonObject.getString("page_path"));
                                }
                            }
                        } catch (Exception e) {
                            logger.warn("❌ 数据解析失败: {}", s);
                        }
                    }
                })
                .name("json-parser-with-second-ts");

        // === 1. 实时路径分析 === (每3条相同路径触发)
        SingleOutputStreamOperator<Tuple3<String, String, Long>> realtimeStats = parsedStream
                .map(new MapFunction<JSONObject, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(JSONObject json) throws Exception {
                        String date = json.getString("log_date");
                        String path = json.getString("page_path");
                        Long timestamp = json.getLong("processed_ts");
                        return Tuple3.of("实时监控", path, 1L);
                    }
                })
                .name("realtime-mapper")
                .keyBy(value -> value.f1) // 按路径分组
                .countWindow(3) // 每3条相同路径的数据触发
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1,
                                                               Tuple3<String, String, Long> value2) {
                        return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
                    }
                })
                .name("realtime-counter");

        // 输出实时结果
        realtimeStats.map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value) throws Exception {
                        String result = String.format("🔴 实时路径 - %s: %d次访问", value.f1, value.f2);
                        return result;
                    }
                })
                .name("realtime-formatter")
                .printToErr();

        // === 2. 当天路径分析 === (每5条相同日期+路径触发)
        SingleOutputStreamOperator<Tuple3<String, String, Long>> todayStats = parsedStream
                .map(new MapFunction<JSONObject, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(JSONObject json) throws Exception {
                        String date = json.getString("log_date");
                        String path = json.getString("page_path");
                        return Tuple3.of(date, path, 1L);
                    }
                })
                .name("today-mapper")
                .keyBy(value -> value.f0 + "_" + value.f1) // 按日期+路径分组
                .countWindow(5) // 每5条触发
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1,
                                                               Tuple3<String, String, Long> value2) {
                        return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
                    }
                })
                .name("today-counter");

        // 输出当天结果
        todayStats.map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value) throws Exception {
                        String result = String.format("🟢 当天统计 [%s] - %s: %d次", value.f0, value.f1, value.f2);
                        return result;
                    }
                })
                .name("today-formatter")
                .printToErr();

        // === 3. 历史路径分析（7天汇总）=== (每8条相同路径触发)
        SingleOutputStreamOperator<Tuple3<String, String, Long>> historyStats = parsedStream
                .map(new MapFunction<JSONObject, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(JSONObject json) throws Exception {
                        String path = json.getString("page_path");
                        return Tuple3.of("历史汇总", path, 1L);
                    }
                })
                .name("history-mapper")
                .keyBy(value -> value.f1) // 按路径分组
                .countWindow(8) // 每8条触发
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1,
                                                               Tuple3<String, String, Long> value2) {
                        return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
                    }
                })
                .name("history-counter");

        // 输出历史结果
        historyStats.map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value) throws Exception {
                        String result = String.format("🟡 历史汇总 - %s: 总计%d次访问", value.f1, value.f2);
                        return result;
                    }
                })
                .name("history-formatter")
                .printToErr();

        // === 4. 日期分布统计 === (每4条相同日期触发)
        SingleOutputStreamOperator<Tuple3<String, String, Long>> dateDistribution = parsedStream
                .map(new MapFunction<JSONObject, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(JSONObject json) throws Exception {
                        String date = json.getString("log_date");
                        return Tuple3.of("日期分布", date, 1L);
                    }
                })
                .name("date-distribution-mapper")
                .keyBy(value -> value.f1) // 按日期分组
                .countWindow(4) // 每4条触发
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1,
                                                               Tuple3<String, String, Long> value2) {
                        return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
                    }
                })
                .name("date-distribution-counter");

        // 输出日期分布
        dateDistribution.map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value) throws Exception {
                        String result = String.format("🔵 日期分布 - %s: %d次访问", value.f1, value.f2);
                        return result;
                    }
                })
                .name("date-distribution-formatter")
                .printToErr();

        // === 5. 热门路径排名 === (每15条所有路径触发)
        SingleOutputStreamOperator<Tuple3<String, String, Long>> hotPathStats = parsedStream
                .map(new MapFunction<JSONObject, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(JSONObject json) throws Exception {
                        String path = json.getString("page_path");
                        return Tuple3.of("热门路径", path, 1L);
                    }
                })
                .name("hot-path-mapper")
                .keyBy(value -> value.f1) // 按路径分组
                .countWindow(15) // 每15条触发
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1,
                                                               Tuple3<String, String, Long> value2) {
                        return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
                    }
                })
                .name("hot-path-counter");

        // 输出热门路径
        hotPathStats.map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value) throws Exception {
                        String result = String.format("🔥 热门路径 - %s: 累计%d次", value.f1, value.f2);
                        return result;
                    }
                })
                .name("hot-path-formatter")
                .printToErr();

        // === 6. 保存路径分析结果到Doris ===
        SingleOutputStreamOperator<String> pathAnalysisForDoris = parsedStream
                .map(new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject json) throws Exception {
                        JSONObject result = new JSONObject();
                        result.put("log_date", json.getString("log_date"));
                        result.put("page_path", json.getString("page_path"));
                        result.put("user_id", json.getString("user_id"));
                        result.put("session_id", json.getString("session_id"));
                        result.put("timestamp", json.getLong("processed_ts"));
                        result.put("analysis_type", "path_analysis");
                        result.put("create_time", System.currentTimeMillis());

                        String jsonStr = result.toJSONString();
                        logger.info("💾 准备写入Doris: {}", jsonStr); // 添加日志
                        return jsonStr;
                    }
                })
                .name("path-analysis-doris-mapper");

        // 保存到Doris
        pathAnalysisForDoris
                .name("doris-path-analysis-sink")
                .sinkTo(DorisSinkUtils.createDorisSink("flink_path_analysis"))
                .name("doris-path-sink")
                .setParallelism(1);  // 确保单并行度，便于调试





        logger.info("✅ 作业配置完成，开始持续运行...");
        logger.info("📊 将实时输出五种分析结果：");
        logger.info("   🔴 实时路径 - 每3次相同路径访问触发");
        logger.info("   🟢 当天统计 - 每5次相同日期+路径访问触发");
        logger.info("   🟡 历史汇总 - 每8次相同路径访问触发");
        logger.info("   🔵 日期分布 - 每4次相同日期访问触发");
        logger.info("   🔥 热门路径 - 每15次相同路径访问触发");

        // 显示Doris建表SQL
        showDorisSQL();

        try {
            env.execute("CompletePathAnalysis");
        } catch (Exception e) {
            logger.error("❌ 作业执行失败: {}", e.getMessage());
        }
    }

    /**
     * 秒级时间戳的模拟用户行为数据源
     */
    private static class SecondTimestampUserBehaviorSource implements SourceFunction<String> {
        private volatile boolean isRunning = true;
        private long count = 0;

        private final String[] pagePaths = {
                "首页->商品列表->商品详情->购物车",
                "首页->搜索->商品详情->立即购买",
                "首页->活动页->商品详情->购物车->结算",
                "首页->分类->商品列表->商品详情",
                "首页->商品详情->加入收藏",
                "首页->品牌页->商品列表->商品详情",
                "首页->推荐页->商品详情->立即购买",
                "首页->促销页->商品列表->购物车"
        };

        // 2025-10-23 到 2025-10-29 的秒级时间戳范围
        private final long startTimestamp = LocalDate.of(2025, 10, 23)
                .atStartOfDay(ZoneId.of(TIME_ZONE))
                .toEpochSecond(); // 1730131200

        private final long endTimestamp = LocalDate.of(2025, 10, 29)
                .atTime(23, 59, 59)
                .atZone(ZoneId.of(TIME_ZONE))
                .toEpochSecond(); // 1730659199

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            logger.info("🎯 开始生成秒级时间戳数据...");
            logger.info("📅 时间范围: 2025-10-23 到 2025-10-29");
            logger.info("⏰ 秒级时间戳范围: {} 到 {}", startTimestamp, endTimestamp);

            while (isRunning) {
                // 在时间范围内随机生成秒级时间戳
                long timestamp = startTimestamp + random.nextInt((int)(endTimestamp - startTimestamp));
                String path = pagePaths[random.nextInt(pagePaths.length)];
                String userId = "user_" + (1000 + random.nextInt(9000));
                String sessionId = "session_" + (10000 + random.nextInt(90000));

                // 转换为日期用于验证
                LocalDate localDate = Instant.ofEpochSecond(timestamp)
                        .atZone(ZoneId.of(TIME_ZONE))
                        .toLocalDate();

                JSONObject json = new JSONObject();
                json.put("ts", timestamp); // 秒级时间戳
                json.put("user_id", userId);
                json.put("page_path", path);
                json.put("session_id", sessionId);
                json.put("log_date", localDate.toString()); // 用于显示的日期
                json.put("log_type", "user_behavior");
                json.put("timestamp_type", "second"); // 标记为秒级

                ctx.collect(json.toJSONString());
                count++;

                if (count % 20 == 0) {
                    logger.info("📈 数据生成进度: 已生成 {} 条秒级数据", count);
                }

                // 控制数据生成速度：每秒2-4条
                Thread.sleep(250 + random.nextInt(250));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
            logger.info("⏹️ 数据源停止，共生成 {} 条秒级时间戳数据", count);
        }
    }

    /**
     * 显示Doris建表SQL和示例数据
     */
    private static void showDorisSQL() {
        String sql =
                "-- =============================================\n" +
                        "-- 历史天+当天路径分析 - Doris建表SQL\n" +
                        "-- =============================================\n\n" +

                        "-- 1. 创建路径分析日表\n" +
                        "CREATE TABLE IF NOT EXISTS path_analysis_daily (\n" +
                        "    log_date DATE NOT NULL COMMENT '日志日期',\n" +
                        "    path_sequence VARCHAR(500) NOT NULL COMMENT '访问路径序列',\n" +
                        "    user_count BIGINT NOT NULL COMMENT '用户访问次数',\n" +
                        "    second_timestamp BIGINT NOT NULL COMMENT '秒级时间戳',\n" +
                        "    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(log_date, path_sequence)\n" +
                        "PARTITION BY RANGE (log_date)\n" +
                        "(\n" +
                        "    PARTITION p20251023 VALUES [('2025-10-23'), ('2025-10-24')),\n" +
                        "    PARTITION p20251024 VALUES [('2025-10-24'), ('2025-10-25')),\n" +
                        "    PARTITION p20251025 VALUES [('2025-10-25'), ('2025-10-26')),\n" +
                        "    PARTITION p20251026 VALUES [('2025-10-26'), ('2025-10-27')),\n" +
                        "    PARTITION p20251027 VALUES [('2025-10-27'), ('2025-10-28')),\n" +
                        "    PARTITION p20251028 VALUES [('2025-10-28'), ('2025-10-29')),\n" +
                        "    PARTITION p20251029 VALUES [('2025-10-29'), ('2025-10-30'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(log_date) BUCKETS 4\n" +
                        "PROPERTIES (\n" +
                        "    \"replication_num\" = \"1\",\n" +
                        "    \"dynamic_partition.enable\" = \"true\",\n" +
                        "    \"dynamic_partition.time_unit\" = \"DAY\",\n" +
                        "    \"dynamic_partition.end\" = \"3\",\n" +
                        "    \"dynamic_partition.prefix\" = \"p\",\n" +
                        "    \"dynamic_partition.buckets\" = \"4\"\n" +
                        ");\n\n" +

                        "-- 2. 创建历史路径汇总表\n" +
                        "CREATE TABLE IF NOT EXISTS path_analysis_history (\n" +
                        "    analysis_date DATE NOT NULL COMMENT '分析日期',\n" +
                        "    path_sequence VARCHAR(500) NOT NULL COMMENT '访问路径序列',\n" +
                        "    total_users BIGINT NOT NULL COMMENT '总用户数',\n" +
                        "    avg_daily_users DOUBLE COMMENT '日均用户数',\n" +
                        "    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(analysis_date, path_sequence)\n" +
                        "DISTRIBUTED BY HASH(analysis_date) BUCKETS 4\n" +
                        "PROPERTIES (\"replication_num\" = \"1\");\n\n" +

                        "-- 3. 插入示例数据\n" +
                        "INSERT INTO path_analysis_daily VALUES\n" +
                        "('2025-10-23', '首页->商品列表->商品详情->购物车', 156, 1730131200, NOW()),\n" +
                        "('2025-10-23', '首页->搜索->商品详情->立即购买', 89, 1730134800, NOW()),\n" +
                        "('2025-10-24', '首页->活动页->商品详情->购物车->结算', 71, 1730217600, NOW()),\n" +
                        "('2025-10-24', '首页->分类->商品列表->商品详情', 221, 1730221200, NOW()),\n" +
                        "('2025-10-25', '首页->商品列表->商品详情->购物车', 178, 1730304000, NOW()),\n" +
                        "('2025-10-25', '首页->搜索->商品详情->立即购买', 105, 1730307600, NOW()),\n" +
                        "('2025-10-26', '首页->活动页->商品详情->购物车->结算', 76, 1730390400, NOW()),\n" +
                        "('2025-10-26', '首页->分类->商品列表->商品详情', 243, 1730394000, NOW()),\n" +
                        "('2025-10-27', '首页->商品列表->商品详情->购物车', 189, 1730476800, NOW()),\n" +
                        "('2025-10-27', '首页->搜索->商品详情->立即购买', 112, 1730480400, NOW()),\n" +
                        "('2025-10-28', '首页->活动页->商品详情->购物车->结算', 79, 1730563200, NOW()),\n" +
                        "('2025-10-28', '首页->分类->商品列表->商品详情', 251, 1730566800, NOW()),\n" +
                        "('2025-10-29', '首页->商品列表->商品详情->购物车', 195, 1730649600, NOW()),\n" +
                        "('2025-10-29', '首页->搜索->商品详情->立即购买', 118, 1730653200, NOW());\n\n" +

                        "-- 4. 查询示例：按日期和路径统计\n" +
                        "SELECT \n" +
                        "    log_date,\n" +
                        "    path_sequence,\n" +
                        "    SUM(user_count) as total_visits,\n" +
                        "    ROUND(AVG(user_count), 2) as avg_daily_visits\n" +
                        "FROM path_analysis_daily \n" +
                        "WHERE log_date BETWEEN '2025-10-23' AND '2025-10-29'\n" +
                        "GROUP BY log_date, path_sequence\n" +
                        "ORDER BY log_date, total_visits DESC;\n\n" +

                        "-- 5. 查询示例：热门路径排名\n" +
                        "SELECT \n" +
                        "    path_sequence,\n" +
                        "    SUM(user_count) as total_visits\n" +
                        "FROM path_analysis_daily\n" +
                        "GROUP BY path_sequence\n" +
                        "ORDER BY total_visits DESC\n" +
                        "LIMIT 10;";

        logger.info("📝 Doris建表SQL（完整版）:\n{}", sql);
    }
}