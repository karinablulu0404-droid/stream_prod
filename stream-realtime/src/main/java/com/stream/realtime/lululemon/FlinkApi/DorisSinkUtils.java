// DorisSinkUtils.java
package com.stream.realtime.lululemon.FlinkApi;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class DorisSinkUtils {
    private static final Logger logger = LoggerFactory.getLogger(DorisSinkUtils.class);

    public static DorisSink<String> createDorisSink(String tableName) {
        try {
            logger.info("🎯 创建Doris Sink - 表: {}", tableName);

            Properties properties = new Properties();
            properties.setProperty("format", "json");
            properties.setProperty("read_json_by_line", "true");
            properties.setProperty("batch.size", "1");
            properties.setProperty("batch.interval.ms", "100");
            properties.setProperty("max_retries", "5");
            properties.setProperty("retry_delay_ms", "1000");
            properties.setProperty("connect.timeout", "10000");
            properties.setProperty("socket.timeout", "10000");
            properties.setProperty("label_prefix", "flink_path_");

            // 修改这里：移除 setEnableDelete 配置
            DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                    .setStreamLoadProp(properties)
                    .setBufferCount(1)
                    .setBufferSize(1024 * 1024) // 1MB
                    .build();

            DorisOptions dorisOptions = DorisOptions.builder()
                    .setFenodes("172.17.42.124:8030")
                    .setTableIdentifier("test." + tableName)
                    .setUsername("root")
                    .setPassword("Wjk19990921.")
                    .build();

            logger.info("🚀 Doris Sink 配置 - FE: 172.17.42.124:8030, 表: test.{}", tableName);

            return DorisSink.<String>builder()
                    .setDorisReadOptions(DorisReadOptions.builder().build())
                    .setDorisExecutionOptions(executionOptions)
                    .setDorisOptions(dorisOptions)
                    .setSerializer(new SimpleStringSerializer())
                    .build();

        } catch (Exception e) {
            logger.error("❌ 创建Doris Sink失败: {}", e.getMessage(), e);
            throw e;
        }
    }
}