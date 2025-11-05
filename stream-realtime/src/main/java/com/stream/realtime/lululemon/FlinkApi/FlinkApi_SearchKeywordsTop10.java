package com.stream.realtime.lululemon.FlinkApi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class FlinkApi_SearchKeywordsTop10 {
    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);

        System.out.println("=== Doris搜索词TOP10分析开始 ===");
        System.out.println("数据源: Doris test.search_keywords");
        System.out.println("时间范围: 历史天 + 当天");
        System.out.println("统计维度: 每天搜索词TOP10\n");

        try {
            // 测试数据库连接并检查数据
            DatabaseCheckResult dbResult = checkDatabaseAndData();

            if (!dbResult.isConnected()) {
                System.out.println("数据库连接测试失败，使用模拟数据进行演示...");
                // 使用模拟数据
                DataSet<SearchKeyword> mockDataSource = env.fromCollection(createMockData());
                DataSet<String> top10Results = mockDataSource
                        .groupBy("ds")
                        .reduceGroup(new DailyTop10GroupReducer());
                top10Results.print();
            } else if (dbResult.getDataCount() == 0) {
                System.out.println("数据库中没有符合条件的数据，使用模拟数据进行演示...");
                // 使用模拟数据
                DataSet<SearchKeyword> mockDataSource = env.fromCollection(createMockData());
                DataSet<String> top10Results = mockDataSource
                        .groupBy("ds")
                        .reduceGroup(new DailyTop10GroupReducer());
                top10Results.print();
            } else {
                System.out.println("数据库连接成功，从Doris读取 " + dbResult.getDataCount() + " 条数据...");
                // 创建Doris数据源
                JdbcInputFormat jdbcInputFormat = createDorisInputFormat();
                DataSource<Row> dorisSource = env.createInput(jdbcInputFormat);

                // 数据处理流程
                DataSet<String> top10Results = dorisSource
                        .map(new RowToSearchKeywordMapper())
                        .groupBy("ds")
                        .reduceGroup(new DailyTop10GroupReducer());

                // 执行并输出结果
                top10Results.print();
            }

            System.out.println("\n=== 分析完成 ===");

        } catch (Exception e) {
            System.err.println("执行过程中发生错误: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 数据库检查结果类
     */
    private static class DatabaseCheckResult {
        private boolean connected;
        private long dataCount;

        public DatabaseCheckResult(boolean connected, long dataCount) {
            this.connected = connected;
            this.dataCount = dataCount;
        }

        public boolean isConnected() { return connected; }
        public long getDataCount() { return dataCount; }
    }

    /**
     * 测试数据库连接并检查数据
     */
    private static DatabaseCheckResult checkDatabaseAndData() {
        String url = "jdbc:mysql://172.17.42.124:9030/test";
        String username = "root";
        String password = "Wjk19990921.";

        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            System.out.println("数据库连接成功!");

            // 检查数据量
            String testQuery = "SELECT COUNT(*) FROM search_keywords WHERE ds >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)";
            try (PreparedStatement stmt = conn.prepareStatement(testQuery);
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    long count = rs.getLong(1);
                    System.out.println("符合条件的数据量: " + count + " 条");
                    return new DatabaseCheckResult(true, count);
                }
            }
            return new DatabaseCheckResult(true, 0);
        } catch (SQLException e) {
            System.err.println("数据库连接失败: " + e.getMessage());
            System.out.println("请检查以下配置:");
            System.out.println("- 数据库地址: " + url);
            System.out.println("- 用户名: " + username);
            System.out.println("- 密码: " + (password.isEmpty() ? "空" : "已设置"));
            System.out.println("- 确保Doris服务正在运行");
            return new DatabaseCheckResult(false, 0);
        }
    }

    /**
     * 创建Doris数据源输入格式
     */
    private static JdbcInputFormat createDorisInputFormat() {
        // Doris连接配置
        String dorisUrl = "jdbc:mysql://172.17.42.124:9030/test";
        String dorisUsername = "root";
        String dorisPassword = "Wjk19990921.";

        // 查询最近2天的数据（历史天+当天）
        String query = "SELECT ds, datetime, keyword, search_count, unique_users " +
                "FROM search_keywords " +
                "WHERE ds >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) " +
                "ORDER BY ds, search_count DESC";

        // 定义字段类型
        TypeInformation[] fieldTypes = new TypeInformation[]{
                BasicTypeInfo.STRING_TYPE_INFO,  // ds
                BasicTypeInfo.STRING_TYPE_INFO,  // datetime
                BasicTypeInfo.STRING_TYPE_INFO,  // keyword
                BasicTypeInfo.LONG_TYPE_INFO,    // search_count
                BasicTypeInfo.LONG_TYPE_INFO     // unique_users
        };

        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

        // 构建JDBC输入格式
        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setDBUrl(dorisUrl)
                .setUsername(dorisUsername)
                .setPassword(dorisPassword)
                .setQuery(query)
                .setRowTypeInfo(rowTypeInfo)
                .finish();

        return jdbcInputFormat;
    }

    /**
     * 创建模拟数据用于测试
     */
    private static List<SearchKeyword> createMockData() {
        List<SearchKeyword> data = new ArrayList<>();

        // 模拟当天数据
        data.add(new SearchKeyword("2024-01-15", "2024-01-15 10:00:00", "lululemon瑜伽裤", 1560L, 890L));
        data.add(new SearchKeyword("2024-01-15", "2024-01-15 11:00:00", "运动内衣", 1340L, 760L));
        data.add(new SearchKeyword("2024-01-15", "2024-01-15 12:00:00", "男士运动服", 980L, 540L));
        data.add(new SearchKeyword("2024-01-15", "2024-01-15 13:00:00", "瑜伽垫", 870L, 430L));
        data.add(new SearchKeyword("2024-01-15", "2024-01-15 14:00:00", "跑步鞋", 760L, 380L));
        data.add(new SearchKeyword("2024-01-15", "2024-01-15 15:00:00", "运动袜", 650L, 320L));
        data.add(new SearchKeyword("2024-01-15", "2024-01-15 16:00:00", "健身包", 540L, 280L));
        data.add(new SearchKeyword("2024-01-15", "2024-01-15 17:00:00", "运动水杯", 430L, 210L));
        data.add(new SearchKeyword("2024-01-15", "2024-01-15 18:00:00", "运动手套", 320L, 180L));
        data.add(new SearchKeyword("2024-01-15", "2024-01-15 19:00:00", "头带", 210L, 150L));
        data.add(new SearchKeyword("2024-01-15", "2024-01-15 20:00:00", "运动毛巾", 180L, 120L));

        // 模拟历史天数据
        data.add(new SearchKeyword("2024-01-14", "2024-01-14 10:00:00", "运动裤", 1200L, 650L));
        data.add(new SearchKeyword("2024-01-14", "2024-01-14 11:00:00", "健身服", 1100L, 580L));
        data.add(new SearchKeyword("2024-01-14", "2024-01-14 12:00:00", "瑜伽服", 950L, 520L));
        data.add(new SearchKeyword("2024-01-14", "2024-01-14 13:00:00", "运动外套", 880L, 490L));
        data.add(new SearchKeyword("2024-01-14", "2024-01-14 14:00:00", "运动短裤", 770L, 410L));
        data.add(new SearchKeyword("2024-01-14", "2024-01-14 15:00:00", "运动T恤", 660L, 350L));
        data.add(new SearchKeyword("2024-01-14", "2024-01-14 16:00:00", "运动鞋", 550L, 290L));
        data.add(new SearchKeyword("2024-01-14", "2024-01-14 17:00:00", "运动帽", 440L, 220L));
        data.add(new SearchKeyword("2024-01-14", "2024-01-14 18:00:00", "运动毛巾", 330L, 190L));
        data.add(new SearchKeyword("2024-01-14", "2024-01-14 19:00:00", "运动护具", 220L, 160L));

        System.out.println("生成模拟数据: " + data.size() + " 条记录");
        return data;
    }

    /**
     * 将Row对象转换为SearchKeyword对象
     */
    public static class RowToSearchKeywordMapper implements MapFunction<Row, SearchKeyword> {
        @Override
        public SearchKeyword map(Row row) throws Exception {
            String ds = row.getField(0) != null ? row.getField(0).toString() : "";
            String datetime = row.getField(1) != null ? row.getField(1).toString() : "";
            String keyword = row.getField(2) != null ? row.getField(2).toString() : "";

            Long searchCount = 0L;
            if (row.getField(3) != null) {
                if (row.getField(3) instanceof Number) {
                    searchCount = ((Number) row.getField(3)).longValue();
                } else if (row.getField(3) instanceof String) {
                    try {
                        searchCount = Long.parseLong(row.getField(3).toString());
                    } catch (NumberFormatException e) {
                        searchCount = 0L;
                    }
                }
            }

            Long uniqueUsers = 0L;
            if (row.getField(4) != null) {
                if (row.getField(4) instanceof Number) {
                    uniqueUsers = ((Number) row.getField(4)).longValue();
                } else if (row.getField(4) instanceof String) {
                    try {
                        uniqueUsers = Long.parseLong(row.getField(4).toString());
                    } catch (NumberFormatException e) {
                        uniqueUsers = 0L;
                    }
                }
            }

            return new SearchKeyword(ds, datetime, keyword, searchCount, uniqueUsers);
        }
    }

    /**
     * 按日期分组计算TOP10搜索词 - 使用GroupReduceFunction
     */
    public static class DailyTop10GroupReducer implements GroupReduceFunction<SearchKeyword, String> {
        @Override
        public void reduce(Iterable<SearchKeyword> values, Collector<String> out) throws Exception {
            Map<String, KeywordStats> keywordStatsMap = new HashMap<>();
            String currentDate = null;

            // 处理同一日期的所有数据
            for (SearchKeyword keyword : values) {
                if (currentDate == null) {
                    currentDate = keyword.getDs();
                }

                // 更新关键词统计（取最大值）
                String keywordText = keyword.getKeyword();
                KeywordStats stats = keywordStatsMap.getOrDefault(keywordText,
                        new KeywordStats(keywordText, currentDate));
                stats.updateStats(keyword.getSearchCount(), keyword.getUniqueUsers());
                keywordStatsMap.put(keywordText, stats);
            }

            if (currentDate != null && !keywordStatsMap.isEmpty()) {
                // 按搜索次数排序取TOP10
                List<KeywordStats> dailyStats = keywordStatsMap.values().stream()
                        .sorted((a, b) -> Long.compare(b.getSearchCount(), a.getSearchCount()))
                        .limit(10)
                        .collect(Collectors.toList());

                // 构建输出结果
                StringBuilder result = new StringBuilder();
                result.append("📊 ").append(currentDate).append(" 搜索词TOP10\n");
                result.append("┌────┬──────────────────────┬────────────┬────────────┐\n");
                result.append("│ 排名 │ 搜索词               │ 搜索次数    │ 搜索人数    │\n");
                result.append("├────┼──────────────────────┼────────────┼────────────┤\n");

                for (int i = 0; i < dailyStats.size(); i++) {
                    KeywordStats stats = dailyStats.get(i);
                    String keyword = stats.getKeyword();
                    if (keyword.length() > 10) {
                        keyword = keyword.substring(0, 10) + "...";
                    }

                    result.append(String.format("│ %2d │ %-20s │ %10d │ %10d │\n",
                            i + 1, keyword, stats.getSearchCount(), stats.getUniqueUsers()));
                }

                result.append("└────┴──────────────────────┴────────────┴────────────┘\n");

                // 添加统计摘要
                long totalSearches = dailyStats.stream().mapToLong(KeywordStats::getSearchCount).sum();
                long totalUsers = dailyStats.stream().mapToLong(KeywordStats::getUniqueUsers).sum();
                result.append(String.format("📈 统计摘要: 总搜索次数: %,d, 总搜索人数: %,d, 平均搜索次数: %.1f\n",
                        totalSearches, totalUsers, (double) totalSearches / dailyStats.size()));

                out.collect(result.toString());
            }
        }
    }

    /**
     * 搜索词数据实体类
     */
    public static class SearchKeyword {
        private String ds;
        private String datetime;
        private String keyword;
        private Long searchCount;
        private Long uniqueUsers;

        public SearchKeyword() {}

        public SearchKeyword(String ds, String datetime, String keyword, Long searchCount, Long uniqueUsers) {
            this.ds = ds;
            this.datetime = datetime;
            this.keyword = keyword;
            this.searchCount = searchCount;
            this.uniqueUsers = uniqueUsers;
        }

        // Getters and Setters
        public String getDs() { return ds; }
        public void setDs(String ds) { this.ds = ds; }

        public String getDatetime() { return datetime; }
        public void setDatetime(String datetime) { this.datetime = datetime; }

        public String getKeyword() { return keyword; }
        public void setKeyword(String keyword) { this.keyword = keyword; }

        public Long getSearchCount() { return searchCount; }
        public void setSearchCount(Long searchCount) { this.searchCount = searchCount; }

        public Long getUniqueUsers() { return uniqueUsers; }
        public void setUniqueUsers(Long uniqueUsers) { this.uniqueUsers = uniqueUsers; }

        @Override
        public String toString() {
            return String.format("SearchKeyword{ds='%s', keyword='%s', searchCount=%d, uniqueUsers=%d}",
                    ds, keyword, searchCount, uniqueUsers);
        }
    }

    /**
     * 关键词统计类
     */
    public static class KeywordStats {
        private String keyword;
        private String date;
        private Long searchCount;
        private Long uniqueUsers;

        public KeywordStats() {}

        public KeywordStats(String keyword, String date) {
            this.keyword = keyword;
            this.date = date;
            this.searchCount = 0L;
            this.uniqueUsers = 0L;
        }

        public void updateStats(Long searchCount, Long uniqueUsers) {
            // 取最大值，避免重复计数
            if (searchCount != null) {
                this.searchCount = Math.max(this.searchCount, searchCount);
            }
            if (uniqueUsers != null) {
                this.uniqueUsers = Math.max(this.uniqueUsers, uniqueUsers);
            }
        }

        // Getters and Setters
        public String getKeyword() { return keyword; }
        public String getDate() { return date; }
        public Long getSearchCount() { return searchCount; }
        public Long getUniqueUsers() { return uniqueUsers; }

        public void setKeyword(String keyword) { this.keyword = keyword; }
        public void setDate(String date) { this.date = date; }
        public void setSearchCount(Long searchCount) { this.searchCount = searchCount; }
        public void setUniqueUsers(Long uniqueUsers) { this.uniqueUsers = uniqueUsers; }

        @Override
        public String toString() {
            return String.format("KeywordStats{keyword='%s', searchCount=%d, uniqueUsers=%d}",
                    keyword, searchCount, uniqueUsers);
        }
    }
}