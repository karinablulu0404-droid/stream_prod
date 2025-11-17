package com.stream.realtime.lululemon.Damopan;

import cn.hutool.json.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.mysql.cj.x.protobuf.MysqlxDatatypes;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashSet;

// CommentData 类定义
class CommentData {
    private MysqlxDatatypes.Scalar.String userId;
    private String orderId;
    private String content;
    private Long timestamp;
    private String username;
    private String commentLevel;
    private boolean isBlacklisted;
    private Set<String> sensitiveWords;

    // 默认构造方法
    public CommentData() {
        this.sensitiveWords = new HashSet<>();
    }

    // getter 和 setter 方法
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }

    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }

    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public String getCommentLevel() { return commentLevel; }
    public void setCommentLevel(String commentLevel) { this.commentLevel = commentLevel; }

    public boolean isBlacklisted() { return isBlacklisted; }
    public void setBlacklisted(boolean blacklisted) { isBlacklisted = blacklisted; }

    public Set<String> getSensitiveWords() { return sensitiveWords; }
    public void setSensitiveWords(Set<String> sensitiveWords) { this.sensitiveWords = sensitiveWords; }

    @Override
    public String toString() {
        return "CommentData{" +
                "userId='" + userId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", content='" + content + '\'' +
                ", timestamp=" + timestamp +
                ", username='" + username + '\'' +
                ", commentLevel='" + commentLevel + '\'' +
                ", isBlacklisted=" + isBlacklisted +
                ", sensitiveWords=" + sensitiveWords +
                '}';
    }
}

// KafkaSink 类定义
class KafkaSink implements SinkFunction<CommentData> {
    @Override
    public void invoke(CommentData value, Context context) {
        // 这里实现将数据发送到 Kafka 的逻辑
        System.out.println("Sending to Kafka: " + value);
        // 实际实现中需要使用 Kafka producer
    }
}

public class SQLServerCDCSourceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 启用 checkpoint
        env.enableCheckpointing(30000);

        SourceFunction<String> sourceFunction = SqlServerSource.<String>builder()
                .hostname("localhost")
                .port(1433)
                .database("your_database")
                .tableList("dbo.comments", "dbo.orders") // 监控多张表
                .username("sa")
                .password("your_password")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        // 添加数据源
        DataStreamSource<String> sqlServerCDCSource = env.addSource(sourceFunction);

        // 处理数据
        sqlServerCDCSource
                .map(new CommentDataMapper())
                .addSink(new KafkaSink());

        env.execute("SQLServer CDC Streaming Job");
    }

    // 数据映射器
    public static class CommentDataMapper implements MapFunction<String, CommentData> {
        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public CommentData map(String value) throws Exception {
            try {
                // 解析 JSON 数据
                JsonNode jsonNode = mapper.readTree(value);

                CommentData comment = new CommentData();

                // 安全地获取字段值
                JsonNode afterNode = jsonNode.get("after");
                if (afterNode != null && !afterNode.isNull()) {
                    comment.setUserId(getSafeText(afterNode, "user_id"));
                    comment.setOrderId(getSafeText(afterNode, "order_id"));
                    comment.setContent(getSafeText(afterNode, "content"));
                    comment.setUsername(getSafeText(afterNode, "username"));
                    comment.setCommentLevel(getSafeText(afterNode, "comment_level"));
                }

                // 获取时间戳
                JsonNode tsMsNode = jsonNode.get("ts_ms");
                if (tsMsNode != null && !tsMsNode.isNull()) {
                    comment.setTimestamp(tsMsNode.asLong());
                } else {
                    comment.setTimestamp(System.currentTimeMillis());
                }

                return comment;
            } catch (Exception e) {
                System.err.println("Error parsing JSON: " + value);
                e.printStackTrace();
                // 返回一个空的 CommentData 对象或根据业务需求处理
                CommentData errorComment = new CommentData();
                errorComment.setContent("PARSE_ERROR");
                return errorComment;
            }
        }

        private String getSafeText(JsonNode node, String fieldName) {
            JsonNode fieldNode = node.get(fieldName);
            return (fieldNode != null && !fieldNode.isNull()) ? fieldNode.asText() : "";
        }
    }
}