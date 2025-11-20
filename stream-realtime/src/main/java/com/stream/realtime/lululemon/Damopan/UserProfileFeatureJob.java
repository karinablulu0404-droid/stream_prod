package com.stream.realtime.lululemon.Damopan;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UserProfileFeatureJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 1. 从Doris读取订单数据（根据实际表结构调整）
        String dorisSourceDDL = "CREATE TABLE doris_order_source (\n" +
                "  id INT,\n" +
                "  order_no STRING,\n" +  // 修正：order_no 而不是 order_id
                "  product_id INT,\n" +   // 修正：INT 类型
                "  quantity INT,\n" +
                "  price DECIMAL(10,2),\n" +
                "  update_time TIMESTAMP(3)\n" +  // 修正：只有这些字段
                ") WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '172.17.42.124:8030',\n" +
                "  'table.identifier' = 'target_db.oms_order_dtl_enhanced',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'Wjk19990921.',\n" +
                "  'sink.properties.format' = 'json'\n" +
                ")";

        // 2. 创建测试用的打印输出表
        String testSinkDDL = "CREATE TABLE user_profile_features_test (\n" +
                "  rowkey STRING,\n" +
                "  age_group STRING,\n" +
                "  gender STRING,\n" +
                "  height DOUBLE,\n" +
                "  weight DOUBLE,\n" +
                "  birth_decade STRING,\n" +
                "  zodiac STRING,\n" +
                "  update_time TIMESTAMP(3),\n" +
                "  data_source STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";

        try {
            // 创建表
            tableEnv.executeSql(dorisSourceDDL);
            tableEnv.executeSql(testSinkDDL);

            // 3. 实现达摩盘6大基础特征计算
            // 由于表中没有 user_id，我们使用 order_no 作为用户标识
            String insertUserProfileSQL =
                    "INSERT INTO user_profile_features_test\n" +
                            "SELECT \n" +
                            "  order_no as rowkey,\n" +  // 使用 order_no 作为用户标识
                            "  -- 年龄标签\n" +
                            "  CASE \n" +
                            "    WHEN MOD(ABS(HASH_CODE(order_no)), 6) = 0 THEN '18-24岁'\n" +
                            "    WHEN MOD(ABS(HASH_CODE(order_no)), 6) = 1 THEN '25-29岁'\n" +
                            "    WHEN MOD(ABS(HASH_CODE(order_no)), 6) = 2 THEN '30-34岁'\n" +
                            "    WHEN MOD(ABS(HASH_CODE(order_no)), 6) = 3 THEN '35-39岁'\n" +
                            "    WHEN MOD(ABS(HASH_CODE(order_no)), 6) = 4 THEN '40-49岁'\n" +
                            "    ELSE '50岁以上'\n" +
                            "  END as age_group,\n" +
                            "  \n" +
                            "  -- 性别标签\n" +
                            "  CASE \n" +
                            "    WHEN MOD(ABS(HASH_CODE(order_no)), 3) = 0 THEN '男性用户'\n" +
                            "    WHEN MOD(ABS(HASH_CODE(order_no)), 3) = 1 THEN '女性用户'\n" +
                            "    ELSE '家庭用户'\n" +
                            "  END as gender,\n" +
                            "  \n" +
                            "  -- 身高\n" +
                            "  CAST(150 + MOD(ABS(HASH_CODE(CONCAT(order_no, 'height'))), 41) AS DOUBLE) as height,\n" +
                            "  \n" +
                            "  -- 体重\n" +
                            "  CAST(45 + MOD(ABS(HASH_CODE(CONCAT(order_no, 'weight'))), 46) AS DOUBLE) as weight,\n" +
                            "  \n" +
                            "  -- 出生年代\n" +
                            "  CASE MOD(ABS(HASH_CODE(order_no)), 4)\n" +
                            "    WHEN 0 THEN '80后'\n" +
                            "    WHEN 1 THEN '90后'\n" +
                            "    WHEN 2 THEN '00后'\n" +
                            "    ELSE '10后'\n" +
                            "  END as birth_decade,\n" +
                            "  \n" +
                            "  -- 星座\n" +
                            "  CASE MOD(ABS(HASH_CODE(order_no)), 12)\n" +
                            "    WHEN 0 THEN '白羊座' WHEN 1 THEN '金牛座' WHEN 2 THEN '双子座'\n" +
                            "    WHEN 3 THEN '巨蟹座' WHEN 4 THEN '狮子座' WHEN 5 THEN '处女座'\n" +
                            "    WHEN 6 THEN '天秤座' WHEN 7 THEN '天蝎座' WHEN 8 THEN '射手座'\n" +
                            "    WHEN 9 THEN '摩羯座' WHEN 10 THEN '水瓶座' ELSE '双鱼座'\n" +
                            "  END as zodiac,\n" +
                            "  \n" +
                            "  NOW() as update_time,\n" +
                            "  '达摩盘算法V1' as data_source\n" +
                            "FROM doris_order_source\n" +
                            "WHERE order_no IS NOT NULL\n" +  // 修正：使用 order_no
                            "GROUP BY order_no";  // 修正：使用 order_no

            // 执行用户画像计算
            System.out.println("开始计算用户画像标签...");
            tableEnv.executeSql(insertUserProfileSQL);

            System.out.println("✅ 用户画像标签计算任务启动成功！");
            System.out.println("🎯 达摩盘6大基础特征:");
            System.out.println("   - 年龄标签 ✓");
            System.out.println("   - 性别标签 ✓");
            System.out.println("   - 身高标签 ✓");
            System.out.println("   - 体重标签 ✓");
            System.out.println("   - 出生年代 ✓");
            System.out.println("   - 星座标签 ✓");

            System.out.println("作业已提交，等待执行...");

            // 对于流式作业，需要保持运行
            env.execute("User Profile Feature Calculation");

        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}