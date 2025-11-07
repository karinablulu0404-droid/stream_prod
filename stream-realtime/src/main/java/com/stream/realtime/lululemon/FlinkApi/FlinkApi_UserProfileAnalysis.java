package com.stream.realtime.lululemon.FlinkApi;

import com.alibaba.fastjson2.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class FlinkApi_UserProfileAnalysis {

    private static final Logger logger = LoggerFactory.getLogger(FlinkApi_UserProfileAnalysis.class);

    private static final String TIME_ZONE = "Asia/Shanghai";
    private static final Random random = new Random();
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    @SneakyThrows
    public static void main(String[] args) {
        logger.info("🚀 启动用户画像行为分析作业 - 登录天数+行为分析");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建持续运行的模拟数据流（秒级时间戳）
        DataStreamSource<String> source = (DataStreamSource<String>) env.addSource(new UserBehaviorSource())
                .name("user-behavior-data-source");

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

                                    LocalDateTime localDateTime = Instant.ofEpochSecond(ts)
                                            .atZone(ZoneId.of(TIME_ZONE))
                                            .toLocalDateTime();
                                    String timeSegment = getTimeSegment(localDateTime);

                                    jsonObject.put("log_date", localDate.toString());
                                    jsonObject.put("time_segment", timeSegment);
                                    jsonObject.put("hour", localDateTime.getHour());
                                    jsonObject.put("processed_ts", ts);

                                    collector.collect(jsonObject);
                                    logger.debug("📥 收到用户行为数据: {} [用户: {}] - 行为: {} - 时间: {}",
                                            localDate,
                                            jsonObject.getString("user_id"),
                                            jsonObject.getString("behavior_type"),
                                            timeSegment);
                                }
                            }
                        } catch (Exception e) {
                            logger.warn("❌ 用户行为数据解析失败: {}", s);
                        }
                    }
                })
                .name("user-behavior-json-parser");

        // === 1. 用户登录天数统计 === (每5条相同用户数据触发)
        SingleOutputStreamOperator<Tuple2<String, String>> userLoginDays = parsedStream
                .filter(json -> "login".equals(json.getString("behavior_type")))
                .map(new MapFunction<JSONObject, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(JSONObject json) throws Exception {
                        String userId = json.getString("user_id");
                        String loginDate = json.getString("log_date");
                        return Tuple2.of(userId, loginDate);
                    }
                })
                .name("login-days-mapper")
                .keyBy(value -> value.f0) // 按用户ID分组
                .countWindow(5) // 每5条触发
                .reduce(new ReduceFunction<Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> reduce(Tuple2<String, String> value1,
                                                         Tuple2<String, String> value2) {
                        // 简单的去重逻辑，实际生产中应该用更复杂的状态管理
                        return value1;
                    }
                })
                .name("login-days-counter");

        // 输出用户登录天数
        userLoginDays.map(new MapFunction<Tuple2<String, String>, String>() {
                    @Override
                    public String map(Tuple2<String, String> value) throws Exception {
                        String result = String.format("🔵 用户登录 - 用户: %s, 登录日期: %s", value.f0, value.f1);
                        return result;
                    }
                })
                .name("login-days-formatter")
                .printToErr();

        // === 2. 用户行为综合分析 === (每8条相同用户数据触发)
        SingleOutputStreamOperator<JSONObject> userBehaviorAnalysis = parsedStream
                .keyBy(json -> json.getString("user_id"))
                .countWindow(8)
                .reduce(new ReduceFunction<JSONObject>() {
                    @Override
                    public JSONObject reduce(JSONObject value1, JSONObject value2) throws Exception {
                        // 合并用户行为数据
                        String userId = value1.getString("user_id");
                        Set<String> loginDates = new HashSet<>();
                        Set<String> behaviorTypes = new HashSet<>();
                        Set<String> timeSegments = new HashSet<>();

                        // 处理value1
                        if (value1.containsKey("login_dates")) {
                            loginDates.addAll(value1.getJSONArray("login_dates").toJavaList(String.class));
                        } else {
                            loginDates.add(value1.getString("log_date"));
                        }

                        if (value1.containsKey("behavior_types")) {
                            behaviorTypes.addAll(value1.getJSONArray("behavior_types").toJavaList(String.class));
                        } else {
                            behaviorTypes.add(value1.getString("behavior_type"));
                        }

                        if (value1.containsKey("time_segments")) {
                            timeSegments.addAll(value1.getJSONArray("time_segments").toJavaList(String.class));
                        } else {
                            timeSegments.add(value1.getString("time_segment"));
                        }

                        // 处理value2
                        loginDates.add(value2.getString("log_date"));
                        behaviorTypes.add(value2.getString("behavior_type"));
                        timeSegments.add(value2.getString("time_segment"));

                        // 构建用户画像
                        JSONObject userProfile = new JSONObject();
                        userProfile.put("user_id", userId);
                        userProfile.put("login_dates", new ArrayList<>(loginDates));
                        userProfile.put("total_login_days", loginDates.size());
                        userProfile.put("behavior_types", new ArrayList<>(behaviorTypes));
                        userProfile.put("time_segments", new ArrayList<>(timeSegments));
                        userProfile.put("has_purchase", behaviorTypes.contains("purchase"));
                        userProfile.put("has_search", behaviorTypes.contains("search"));
                        userProfile.put("has_browse", behaviorTypes.contains("browse"));
                        userProfile.put("has_login", behaviorTypes.contains("login"));
                        userProfile.put("analysis_time", System.currentTimeMillis());
                        userProfile.put("data_type", "user_profile");

                        return userProfile;
                    }
                })
                .name("user-behavior-analyzer");

        // 输出用户行为分析结果
        userBehaviorAnalysis.map(new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject userProfile) throws Exception {
                        String userId = userProfile.getString("user_id");
                        int loginDays = userProfile.getInteger("total_login_days");
                        boolean hasPurchase = userProfile.getBoolean("has_purchase");
                        boolean hasSearch = userProfile.getBoolean("has_search");
                        boolean hasBrowse = userProfile.getBoolean("has_browse");

                        String result = String.format("🎯 用户画像 - 用户: %s, 登录%d天, 购买: %s, 搜索: %s, 浏览: %s",
                                userId, loginDays, hasPurchase ? "是" : "否",
                                hasSearch ? "是" : "否", hasBrowse ? "是" : "否");
                        return result;
                    }
                })
                .name("user-profile-formatter")
                .printToErr();

        // === 3. 用户行为时间分布 === (每6条相同时间段数据触发)
        SingleOutputStreamOperator<Tuple2<String, Long>> timeDistribution = parsedStream
                .map(new MapFunction<JSONObject, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(JSONObject json) throws Exception {
                        String timeSegment = json.getString("time_segment");
                        return Tuple2.of(timeSegment, 1L);
                    }
                })
                .name("time-distribution-mapper")
                .keyBy(value -> value.f0)
                .countWindow(6)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1,
                                                       Tuple2<String, Long> value2) {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .name("time-distribution-counter");

        // 输出时间分布
        timeDistribution.map(new MapFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public String map(Tuple2<String, Long> value) throws Exception {
                        String result = String.format("⏰ 时间段分布 - %s: %d次行为", value.f0, value.f1);
                        return result;
                    }
                })
                .name("time-distribution-formatter")
                .printToErr();

        // === 4. 用户完整画像输出（用于ES）===
        SingleOutputStreamOperator<String> esOutput = userBehaviorAnalysis
                .map(new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject userProfile) throws Exception {
                        // 添加ES相关的元数据
                        userProfile.put("_index", "user_profiles");
                        userProfile.put("_type", "_doc");
                        userProfile.put("timestamp", System.currentTimeMillis());

                        return userProfile.toJSONString();
                    }
                })
                .name("es-output-formatter");

        // 输出ES格式数据
        esOutput
                .name("es-output-stream")
                .printToErr();

        logger.info("✅ 用户画像分析作业配置完成，开始持续运行...");
        logger.info("📊 将实时输出四种分析结果：");
        logger.info("   🔵 用户登录 - 每5次用户登录触发");
        logger.info("   🎯 用户画像 - 每8次用户行为触发综合分析");
        logger.info("   ⏰ 时间段分布 - 每6次相同时段行为触发");
        logger.info("   📝 ES数据 - 完整的用户画像JSON数据");

        try {
            env.execute("UserProfileBehaviorAnalysis");
        } catch (Exception e) {
            logger.error("❌ 用户画像分析作业执行失败: {}", e.getMessage());
        }
    }

    /**
     * 获取时间段
     */
    private static String getTimeSegment(LocalDateTime dateTime) {
        int hour = dateTime.getHour();
        if (hour >= 6 && hour < 12) {
            return "morning";
        } else if (hour >= 12 && hour < 14) {
            return "noon";
        } else if (hour >= 14 && hour < 18) {
            return "afternoon";
        } else if (hour >= 18 && hour < 22) {
            return "evening";
        } else {
            return "night";
        }
    }

    /**
     * 模拟用户行为数据源
     */
    private static class UserBehaviorSource implements SourceFunction<String> {
        private volatile boolean isRunning = true;
        private long count = 0;

        private final String[] behaviorTypes = {
                "login", "browse", "search", "purchase", "logout"
        };

        private final String[] pageCategories = {
                "home", "product_list", "product_detail", "shopping_cart",
                "payment", "search_results", "category", "promotion"
        };

        // 2025-10-23 到 2025-10-29 的秒级时间戳范围
        private final long startTimestamp = LocalDate.of(2025, 10, 23)
                .atStartOfDay(ZoneId.of(TIME_ZONE))
                .toEpochSecond();

        private final long endTimestamp = LocalDate.of(2025, 10, 29)
                .atTime(23, 59, 59)
                .atZone(ZoneId.of(TIME_ZONE))
                .toEpochSecond();

        // 模拟用户池
        private final String[] userPool = generateUserPool(50);

        private String[] generateUserPool(int size) {
            String[] users = new String[size];
            for (int i = 0; i < size; i++) {
                users[i] = "user_" + (10000 + i);
            }
            return users;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            logger.info("🎯 开始生成用户行为数据...");
            logger.info("📅 时间范围: 2025-10-23 到 2025-10-29");
            logger.info("👥 用户池大小: {} 个用户", userPool.length);
            logger.info("🎭 行为类型: {}", Arrays.toString(behaviorTypes));

            while (isRunning) {
                // 在时间范围内随机生成秒级时间戳
                long timestamp = startTimestamp + random.nextInt((int)(endTimestamp - startTimestamp));
                String userId = userPool[random.nextInt(userPool.length)];
                String behaviorType = behaviorTypes[random.nextInt(behaviorTypes.length)];
                String pageCategory = pageCategories[random.nextInt(pageCategories.length)];

                // 转换为日期和时间段
                LocalDateTime dateTime = Instant.ofEpochSecond(timestamp)
                        .atZone(ZoneId.of(TIME_ZONE))
                        .toLocalDateTime();
                String timeSegment = getTimeSegment(dateTime);

                JSONObject json = new JSONObject();
                json.put("ts", timestamp);
                json.put("user_id", userId);
                json.put("behavior_type", behaviorType);
                json.put("page_category", pageCategory);
                json.put("time_segment", timeSegment);
                json.put("hour", dateTime.getHour());
                json.put("session_id", "session_" + (10000 + random.nextInt(90000)));
                json.put("device_type", random.nextBoolean() ? "mobile" : "desktop");
                json.put("log_date", dateTime.toLocalDate().toString());
                json.put("log_time", dateTime.format(TIME_FORMATTER));
                json.put("data_type", "user_behavior");

                ctx.collect(json.toJSONString());
                count++;

                if (count % 30 == 0) {
                    logger.info("📈 用户行为数据生成进度: 已生成 {} 条数据", count);
                }

                // 控制数据生成速度：每秒1-3条
                Thread.sleep(330 + random.nextInt(330));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
            logger.info("⏹️ 用户行为数据源停止，共生成 {} 条数据", count);
        }
    }

    /**
     * 显示Doris建表SQL和示例数据
     */
    private static void showDorisSQL() {
        String sql =
                "-- =============================================\n" +
                        "-- 用户画像行为分析 - Doris建表SQL\n" +
                        "-- =============================================\n\n" +

                        "-- 1. 创建用户行为日志表\n" +
                        "CREATE TABLE IF NOT EXISTS user_behavior_logs (\n" +
                        "    log_date DATE NOT NULL COMMENT '日志日期',\n" +
                        "    user_id VARCHAR(50) NOT NULL COMMENT '用户ID',\n" +
                        "    behavior_type VARCHAR(20) NOT NULL COMMENT '行为类型',\n" +
                        "    page_category VARCHAR(50) COMMENT '页面分类',\n" +
                        "    time_segment VARCHAR(20) COMMENT '时间段',\n" +
                        "    hour TINYINT COMMENT '小时',\n" +
                        "    session_id VARCHAR(100) COMMENT '会话ID',\n" +
                        "    device_type VARCHAR(20) COMMENT '设备类型',\n" +
                        "    second_timestamp BIGINT NOT NULL COMMENT '秒级时间戳',\n" +
                        "    log_time VARCHAR(20) COMMENT '具体时间',\n" +
                        "    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(log_date, user_id, behavior_type)\n" +
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
                        "DISTRIBUTED BY HASH(log_date, user_id) BUCKETS 8\n" +
                        "PROPERTIES (\n" +
                        "    \"replication_num\" = \"1\",\n" +
                        "    \"dynamic_partition.enable\" = \"true\",\n" +
                        "    \"dynamic_partition.time_unit\" = \"DAY\",\n" +
                        "    \"dynamic_partition.end\" = \"3\",\n" +
                        "    \"dynamic_partition.prefix\" = \"p\",\n" +
                        "    \"dynamic_partition.buckets\" = \"8\"\n" +
                        ");\n\n" +

                        "-- 2. 创建用户画像汇总表\n" +
                        "CREATE TABLE IF NOT EXISTS user_profiles (\n" +
                        "    user_id VARCHAR(50) NOT NULL COMMENT '用户ID',\n" +
                        "    login_dates JSON COMMENT '登录日期列表',\n" +
                        "    total_login_days INT COMMENT '总登录天数',\n" +
                        "    behavior_types JSON COMMENT '行为类型列表',\n" +
                        "    time_segments JSON COMMENT '时间段列表',\n" +
                        "    has_purchase BOOLEAN COMMENT '是否有购买行为',\n" +
                        "    has_search BOOLEAN COMMENT '是否有搜索行为',\n" +
                        "    has_browse BOOLEAN COMMENT '是否有浏览行为',\n" +
                        "    has_login BOOLEAN COMMENT '是否有登录行为',\n" +
                        "    favorite_time_segment VARCHAR(20) COMMENT '最活跃时间段',\n" +
                        "    last_activity_date DATE COMMENT '最后活动日期',\n" +
                        "    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',\n" +
                        "    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(user_id)\n" +
                        "DISTRIBUTED BY HASH(user_id) BUCKETS 6\n" +
                        "PROPERTIES (\"replication_num\" = \"1\");\n\n" +

                        "-- 3. 插入示例数据\n" +
                        "INSERT INTO user_behavior_logs VALUES\n" +
                        "('2025-10-23', 'user_10001', 'login', 'home', 'morning', 8, 'session_10001', 'mobile', 1730131200, '08:15:23', NOW()),\n" +
                        "('2025-10-23', 'user_10001', 'browse', 'product_list', 'morning', 8, 'session_10001', 'mobile', 1730131320, '08:22:00', NOW()),\n" +
                        "('2025-10-23', 'user_10001', 'search', 'search_results', 'morning', 9, 'session_10001', 'mobile', 1730134800, '09:15:45', NOW()),\n" +
                        "('2025-10-23', 'user_10001', 'purchase', 'payment', 'morning', 10, 'session_10001', 'mobile', 1730138400, '10:20:30', NOW()),\n" +
                        "('2025-10-24', 'user_10001', 'login', 'home', 'afternoon', 15, 'session_10002', 'desktop', 1730221200, '15:15:10', NOW()),\n" +
                        "('2025-10-24', 'user_10001', 'browse', 'product_detail', 'afternoon', 15, 'session_10002', 'desktop', 1730221800, '15:30:05', NOW()),\n" +
                        "('2025-10-25', 'user_10002', 'login', 'home', 'evening', 20, 'session_10003', 'mobile', 1730307600, '20:05:15', NOW()),\n" +
                        "('2025-10-25', 'user_10002', 'search', 'search_results', 'evening', 20, 'session_10003', 'mobile', 1730308200, '20:10:20', NOW()),\n" +
                        "('2025-10-25', 'user_10002', 'browse', 'category', 'evening', 21, 'session_10003', 'mobile', 1730311200, '21:00:45', NOW()),\n" +
                        "('2025-10-26', 'user_10003', 'login', 'home', 'morning', 9, 'session_10004', 'desktop', 1730394000, '09:15:30', NOW()),\n" +
                        "('2025-10-26', 'user_10003', 'purchase', 'payment', 'morning', 10, 'session_10004', 'desktop', 1730397600, '10:20:15', NOW()),\n" +
                        "('2025-10-27', 'user_10001', 'login', 'home', 'noon', 12, 'session_10005', 'mobile', 1730480400, '12:10:25', NOW()),\n" +
                        "('2025-10-27', 'user_10004', 'login', 'home', 'afternoon', 16, 'session_10006', 'mobile', 1730487600, '16:15:40', NOW()),\n" +
                        "('2025-10-28', 'user_10002', 'login', 'home', 'night', 23, 'session_10007', 'mobile', 1730574000, '23:05:55', NOW()),\n" +
                        "('2025-10-29', 'user_10005', 'login', 'home', 'morning', 7, 'session_10008', 'desktop', 1730653200, '07:25:10', NOW()),\n" +
                        "('2025-10-29', 'user_10005', 'search', 'search_results', 'morning', 8, 'session_10008', 'desktop', 1730656800, '08:15:35', NOW()),\n" +
                        "('2025-10-29', 'user_10005', 'purchase', 'payment', 'morning', 9, 'session_10008', 'desktop', 1730660400, '09:25:50', NOW());\n\n" +

                        "-- 4. 查询示例：用户登录天数统计\n" +
                        "SELECT \n" +
                        "    user_id,\n" +
                        "    COUNT(DISTINCT log_date) as login_days,\n" +
                        "    GROUP_CONCAT(DISTINCT log_date ORDER BY log_date) as login_dates\n" +
                        "FROM user_behavior_logs \n" +
                        "WHERE behavior_type = 'login' \n" +
                        "  AND log_date BETWEEN '2025-10-23' AND '2025-10-29'\n" +
                        "GROUP BY user_id\n" +
                        "ORDER BY login_days DESC;\n\n" +

                        "-- 5. 查询示例：用户行为分析\n" +
                        "SELECT \n" +
                        "    user_id,\n" +
                        "    COUNT(DISTINCT log_date) as active_days,\n" +
                        "    SUM(CASE WHEN behavior_type = 'purchase' THEN 1 ELSE 0 END) as purchase_count,\n" +
                        "    SUM(CASE WHEN behavior_type = 'search' THEN 1 ELSE 0 END) as search_count,\n" +
                        "    SUM(CASE WHEN behavior_type = 'browse' THEN 1 ELSE 0 END) as browse_count,\n" +
                        "    MAX(log_date) as last_activity_date\n" +
                        "FROM user_behavior_logs\n" +
                        "WHERE log_date BETWEEN '2025-10-23' AND '2025-10-29'\n" +
                        "GROUP BY user_id\n" +
                        "ORDER BY active_days DESC, purchase_count DESC;\n\n" +

                        "-- 6. 查询示例：用户活跃时间段分析\n" +
                        "SELECT \n" +
                        "    time_segment,\n" +
                        "    COUNT(*) as behavior_count,\n" +
                        "    COUNT(DISTINCT user_id) as active_users\n" +
                        "FROM user_behavior_logs\n" +
                        "WHERE log_date BETWEEN '2025-10-23' AND '2025-10-29'\n" +
                        "GROUP BY time_segment\n" +
                        "ORDER BY behavior_count DESC;\n\n" +

                        "-- 7. 创建ES索引的映射建议\n" +
                        "{\n" +
                        "  \"mappings\": {\n" +
                        "    \"properties\": {\n" +
                        "      \"user_id\": { \"type\": \"keyword\" },\n" +
                        "      \"login_dates\": { \"type\": \"date\", \"format\": \"yyyy-MM-dd\" },\n" +
                        "      \"total_login_days\": { \"type\": \"integer\" },\n" +
                        "      \"behavior_types\": { \"type\": \"keyword\" },\n" +
                        "      \"time_segments\": { \"type\": \"keyword\" },\n" +
                        "      \"has_purchase\": { \"type\": \"boolean\" },\n" +
                        "      \"has_search\": { \"type\": \"boolean\" },\n" +
                        "      \"has_browse\": { \"type\": \"boolean\" },\n" +
                        "      \"has_login\": { \"type\": \"boolean\" },\n" +
                        "      \"analysis_time\": { \"type\": \"date\" },\n" +
                        "      \"data_type\": { \"type\": \"keyword\" }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}";

        logger.info("📝 Doris用户画像建表SQL（完整版）:\n{}", sql);
    }

    static {
        showDorisSQL();
    }
}