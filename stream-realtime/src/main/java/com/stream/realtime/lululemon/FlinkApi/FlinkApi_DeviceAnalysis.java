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

public class FlinkApi_DeviceAnalysis {

    private static final Logger logger = LoggerFactory.getLogger(FlinkApi_DeviceAnalysis.class);

    private static final String TIME_ZONE = "Asia/Shanghai";
    private static final Random random = new Random();

    @SneakyThrows
    public static void main(String[] args) {
        logger.info("🚀 启动完整版用户设备分析作业 - 历史天+当天设备统计");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建持续运行的模拟数据流（秒级时间戳）
        DataStreamSource<String> source = (DataStreamSource<String>) env.addSource(new SecondTimestampDeviceSource())
                .name("second-timestamp-device-source");

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
                                    logger.debug("📥 收到设备数据: {} [ts: {}] - {} - {}",
                                            localDate, ts,
                                            jsonObject.getString("device_type"),
                                            jsonObject.getString("device_model"));
                                }
                            }
                        } catch (Exception e) {
                            logger.warn("❌ 设备数据解析失败: {}", s);
                        }
                    }
                })
                .name("device-json-parser-with-second-ts");

        // === 1. 实时设备类型统计 === (每3条相同设备类型触发)
        SingleOutputStreamOperator<Tuple3<String, String, Long>> realtimeDeviceTypeStats = parsedStream
                .map(new MapFunction<JSONObject, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(JSONObject json) throws Exception {
                        String deviceType = json.getString("device_type");
                        return Tuple3.of("实时设备类型", deviceType, 1L);
                    }
                })
                .name("realtime-device-type-mapper")
                .keyBy(value -> value.f1) // 按设备类型分组
                .countWindow(3) // 每3条相同设备类型的数据触发
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1,
                                                               Tuple3<String, String, Long> value2) {
                        return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
                    }
                })
                .name("realtime-device-type-counter");

        // 输出实时设备类型结果
        realtimeDeviceTypeStats.map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value) throws Exception {
                        String result = String.format("🔴 实时设备类型 - %s: %d次访问", value.f1, value.f2);
                        return result;
                    }
                })
                .name("realtime-device-type-formatter")
                .printToErr();

        // === 2. 当天设备统计 === (每5条相同日期+设备类型触发)
        SingleOutputStreamOperator<Tuple3<String, String, Long>> todayDeviceStats = parsedStream
                .map(new MapFunction<JSONObject, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(JSONObject json) throws Exception {
                        String date = json.getString("log_date");
                        String deviceType = json.getString("device_type");
                        return Tuple3.of(date, deviceType, 1L);
                    }
                })
                .name("today-device-mapper")
                .keyBy(value -> value.f0 + "_" + value.f1) // 按日期+设备类型分组
                .countWindow(5) // 每5条触发
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1,
                                                               Tuple3<String, String, Long> value2) {
                        return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
                    }
                })
                .name("today-device-counter");

        // 输出当天设备结果
        todayDeviceStats.map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value) throws Exception {
                        String result = String.format("🟢 当天设备统计 [%s] - %s: %d次", value.f0, value.f1, value.f2);
                        return result;
                    }
                })
                .name("today-device-formatter")
                .printToErr();

        // === 3. 历史设备类型汇总（7天汇总）=== (每8条相同设备类型触发)
        SingleOutputStreamOperator<Tuple3<String, String, Long>> historyDeviceStats = parsedStream
                .map(new MapFunction<JSONObject, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(JSONObject json) throws Exception {
                        String deviceType = json.getString("device_type");
                        return Tuple3.of("历史设备汇总", deviceType, 1L);
                    }
                })
                .name("history-device-mapper")
                .keyBy(value -> value.f1) // 按设备类型分组
                .countWindow(8) // 每8条触发
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1,
                                                               Tuple3<String, String, Long> value2) {
                        return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
                    }
                })
                .name("history-device-counter");

        // 输出历史设备结果
        historyDeviceStats.map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value) throws Exception {
                        String result = String.format("🟡 历史设备汇总 - %s: 总计%d次访问", value.f1, value.f2);
                        return result;
                    }
                })
                .name("history-device-formatter")
                .printToErr();

        // === 4. 设备型号统计 === (每6条相同设备型号触发)
        SingleOutputStreamOperator<Tuple3<String, String, Long>> deviceModelStats = parsedStream
                .map(new MapFunction<JSONObject, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(JSONObject json) throws Exception {
                        String deviceModel = json.getString("device_model");
                        return Tuple3.of("设备型号统计", deviceModel, 1L);
                    }
                })
                .name("device-model-mapper")
                .keyBy(value -> value.f1) // 按设备型号分组
                .countWindow(6) // 每6条触发
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1,
                                                               Tuple3<String, String, Long> value2) {
                        return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
                    }
                })
                .name("device-model-counter");

        // 输出设备型号结果
        deviceModelStats.map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value) throws Exception {
                        String result = String.format("🔵 设备型号统计 - %s: %d次访问", value.f1, value.f2);
                        return result;
                    }
                })
                .name("device-model-formatter")
                .printToErr();

        // === 5. 操作系统统计 === (每10条相同操作系统触发)
        SingleOutputStreamOperator<Tuple3<String, String, Long>> osStats = parsedStream
                .map(new MapFunction<JSONObject, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(JSONObject json) throws Exception {
                        String os = json.getString("os");
                        return Tuple3.of("操作系统统计", os, 1L);
                    }
                })
                .name("os-mapper")
                .keyBy(value -> value.f1) // 按操作系统分组
                .countWindow(10) // 每10条触发
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1,
                                                               Tuple3<String, String, Long> value2) {
                        return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
                    }
                })
                .name("os-counter");

        // 输出操作系统结果
        osStats.map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value) throws Exception {
                        String result = String.format("🟣 操作系统统计 - %s: %d次访问", value.f1, value.f2);
                        return result;
                    }
                })
                .name("os-formatter")
                .printToErr();

        logger.info("✅ 设备分析作业配置完成，开始持续运行...");
        logger.info("📊 将实时输出五种设备分析结果：");
        logger.info("   🔴 实时设备类型 - 每3次相同设备类型访问触发");
        logger.info("   🟢 当天设备统计 - 每5次相同日期+设备类型访问触发");
        logger.info("   🟡 历史设备汇总 - 每8次相同设备类型访问触发");
        logger.info("   🔵 设备型号统计 - 每6次相同设备型号访问触发");
        logger.info("   🟣 操作系统统计 - 每10次相同操作系统访问触发");

        try {
            env.execute("CompleteDeviceAnalysis");
        } catch (Exception e) {
            logger.error("❌ 设备分析作业执行失败: {}", e.getMessage());
        }
    }

    /**
     * 秒级时间戳的模拟用户设备数据源
     */
    private static class SecondTimestampDeviceSource implements SourceFunction<String> {
        private volatile boolean isRunning = true;
        private long count = 0;

        private final String[] deviceTypes = {
                "Mobile", "Tablet", "Desktop", "Smart TV", "Wearable"
        };

        private final String[] deviceModels = {
                "iPhone 15", "Samsung Galaxy S24", "iPad Pro", "MacBook Pro",
                "Huawei Mate 60", "Xiaomi 14", "Google Pixel 8", "OnePlus 12",
                "Windows PC", "Android Tablet", "Smart Watch", "Gaming Console"
        };

        private final String[] operatingSystems = {
                "iOS 17", "Android 14", "Windows 11", "macOS Sonoma",
                "HarmonyOS", "Chrome OS", "Wear OS", "tvOS"
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
            logger.info("🎯 开始生成秒级时间戳设备数据...");
            logger.info("📅 时间范围: 2025-10-23 到 2025-10-29");
            logger.info("⏰ 秒级时间戳范围: {} 到 {}", startTimestamp, endTimestamp);

            while (isRunning) {
                // 在时间范围内随机生成秒级时间戳
                long timestamp = startTimestamp + random.nextInt((int)(endTimestamp - startTimestamp));
                String deviceType = deviceTypes[random.nextInt(deviceTypes.length)];
                String deviceModel = deviceModels[random.nextInt(deviceModels.length)];
                String os = operatingSystems[random.nextInt(operatingSystems.length)];
                String userId = "user_" + (1000 + random.nextInt(9000));

                // 转换为日期用于验证
                LocalDate localDate = Instant.ofEpochSecond(timestamp)
                        .atZone(ZoneId.of(TIME_ZONE))
                        .toLocalDate();

                JSONObject json = new JSONObject();
                json.put("ts", timestamp); // 秒级时间戳
                json.put("user_id", userId);
                json.put("device_type", deviceType);
                json.put("device_model", deviceModel);
                json.put("os", os);
                json.put("log_date", localDate.toString()); // 用于显示的日期
                json.put("log_type", "device_behavior");
                json.put("timestamp_type", "second"); // 标记为秒级

                ctx.collect(json.toJSONString());
                count++;

                if (count % 20 == 0) {
                    logger.info("📈 设备数据生成进度: 已生成 {} 条秒级设备数据", count);
                }

                // 控制数据生成速度：每秒2-4条
                Thread.sleep(250 + random.nextInt(250));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
            logger.info("⏹️ 设备数据源停止，共生成 {} 条秒级时间戳设备数据", count);
        }
    }

    /**
     * 显示Doris建表SQL和示例数据
     */
    private static void showDorisSQL() {
        String sql =
                "-- =============================================\n" +
                        "-- 历史天+当天用户设备分析 - Doris建表SQL\n" +
                        "-- =============================================\n\n" +

                        "-- 1. 创建设备分析日表\n" +
                        "CREATE TABLE IF NOT EXISTS device_analysis_daily (\n" +
                        "    log_date DATE NOT NULL COMMENT '日志日期',\n" +
                        "    device_type VARCHAR(50) NOT NULL COMMENT '设备类型',\n" +
                        "    device_model VARCHAR(100) COMMENT '设备型号',\n" +
                        "    os VARCHAR(50) COMMENT '操作系统',\n" +
                        "    user_count BIGINT NOT NULL COMMENT '用户访问次数',\n" +
                        "    second_timestamp BIGINT NOT NULL COMMENT '秒级时间戳',\n" +
                        "    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(log_date, device_type, device_model)\n" +
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

                        "-- 2. 创建设备历史汇总表\n" +
                        "CREATE TABLE IF NOT EXISTS device_analysis_history (\n" +
                        "    analysis_date DATE NOT NULL COMMENT '分析日期',\n" +
                        "    device_type VARCHAR(50) NOT NULL COMMENT '设备类型',\n" +
                        "    total_users BIGINT NOT NULL COMMENT '总用户数',\n" +
                        "    avg_daily_users DOUBLE COMMENT '日均用户数',\n" +
                        "    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(analysis_date, device_type)\n" +
                        "DISTRIBUTED BY HASH(analysis_date) BUCKETS 4\n" +
                        "PROPERTIES (\"replication_num\" = \"1\");\n\n" +

                        "-- 3. 创建设备型号统计表\n" +
                        "CREATE TABLE IF NOT EXISTS device_model_analysis (\n" +
                        "    log_date DATE NOT NULL COMMENT '日志日期',\n" +
                        "    device_model VARCHAR(100) NOT NULL COMMENT '设备型号',\n" +
                        "    user_count BIGINT NOT NULL COMMENT '用户访问次数',\n" +
                        "    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(log_date, device_model)\n" +
                        "DISTRIBUTED BY HASH(log_date) BUCKETS 4\n" +
                        "PROPERTIES (\"replication_num\" = \"1\");\n\n" +

                        "-- 4. 插入示例数据\n" +
                        "INSERT INTO device_analysis_daily VALUES\n" +
                        "('2025-10-23', 'Mobile', 'iPhone 15', 'iOS 17', 234, 1730131200, NOW()),\n" +
                        "('2025-10-23', 'Desktop', 'MacBook Pro', 'macOS Sonoma', 156, 1730134800, NOW()),\n" +
                        "('2025-10-23', 'Tablet', 'iPad Pro', 'iOS 17', 89, 1730138400, NOW()),\n" +
                        "('2025-10-24', 'Mobile', 'Samsung Galaxy S24', 'Android 14', 278, 1730217600, NOW()),\n" +
                        "('2025-10-24', 'Desktop', 'Windows PC', 'Windows 11', 134, 1730221200, NOW()),\n" +
                        "('2025-10-24', 'Mobile', 'Huawei Mate 60', 'HarmonyOS', 167, 1730224800, NOW()),\n" +
                        "('2025-10-25', 'Tablet', 'Android Tablet', 'Android 14', 76, 1730304000, NOW()),\n" +
                        "('2025-10-25', 'Mobile', 'Xiaomi 14', 'Android 14', 198, 1730307600, NOW()),\n" +
                        "('2025-10-25', 'Desktop', 'MacBook Pro', 'macOS Sonoma', 145, 1730311200, NOW()),\n" +
                        "('2025-10-26', 'Mobile', 'Google Pixel 8', 'Android 14', 223, 1730390400, NOW()),\n" +
                        "('2025-10-26', 'Smart TV', 'Samsung Smart TV', 'Tizen', 45, 1730394000, NOW()),\n" +
                        "('2025-10-26', 'Wearable', 'Smart Watch', 'Wear OS', 32, 1730397600, NOW()),\n" +
                        "('2025-10-27', 'Mobile', 'OnePlus 12', 'Android 14', 187, 1730476800, NOW()),\n" +
                        "('2025-10-27', 'Desktop', 'Windows PC', 'Windows 11', 156, 1730480400, NOW()),\n" +
                        "('2025-10-27', 'Tablet', 'iPad Pro', 'iOS 17', 92, 1730484000, NOW()),\n" +
                        "('2025-10-28', 'Mobile', 'iPhone 15', 'iOS 17', 256, 1730563200, NOW()),\n" +
                        "('2025-10-28', 'Mobile', 'Samsung Galaxy S24', 'Android 14', 234, 1730566800, NOW()),\n" +
                        "('2025-10-28', 'Desktop', 'MacBook Pro', 'macOS Sonoma', 167, 1730570400, NOW()),\n" +
                        "('2025-10-29', 'Mobile', 'Huawei Mate 60', 'HarmonyOS', 189, 1730649600, NOW()),\n" +
                        "('2025-10-29', 'Tablet', 'Android Tablet', 'Android 14', 78, 1730653200, NOW());\n\n" +

                        "-- 5. 查询示例：按日期和设备类型统计\n" +
                        "SELECT \n" +
                        "    log_date,\n" +
                        "    device_type,\n" +
                        "    SUM(user_count) as total_visits,\n" +
                        "    ROUND(AVG(user_count), 2) as avg_daily_visits\n" +
                        "FROM device_analysis_daily \n" +
                        "WHERE log_date BETWEEN '2025-10-23' AND '2025-10-29'\n" +
                        "GROUP BY log_date, device_type\n" +
                        "ORDER BY log_date, total_visits DESC;\n\n" +

                        "-- 6. 查询示例：热门设备类型排名\n" +
                        "SELECT \n" +
                        "    device_type,\n" +
                        "    SUM(user_count) as total_visits\n" +
                        "FROM device_analysis_daily\n" +
                        "GROUP BY device_type\n" +
                        "ORDER BY total_visits DESC;\n\n" +

                        "-- 7. 查询示例：设备型号排名\n" +
                        "SELECT \n" +
                        "    device_model,\n" +
                        "    SUM(user_count) as total_visits\n" +
                        "FROM device_analysis_daily\n" +
                        "GROUP BY device_model\n" +
                        "ORDER BY total_visits DESC\n" +
                        "LIMIT 10;\n\n" +

                        "-- 8. 查询示例：操作系统分布\n" +
                        "SELECT \n" +
                        "    os,\n" +
                        "    SUM(user_count) as total_visits\n" +
                        "FROM device_analysis_daily\n" +
                        "GROUP BY os\n" +
                        "ORDER BY total_visits DESC;";

        logger.info("📝 Doris设备分析建表SQL（完整版）:\n{}", sql);
    }

    static {
        showDorisSQL();
    }
}