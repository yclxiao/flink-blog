package top.mangod.flinkblog.demo006sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class StreamBatchUnifiedDemo {
    public static void main(String[] args) throws Exception {
        // 设置流处理的环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Kafka 流处理表
        String createKafkaSourceDDL = "CREATE TABLE kafka_stream_orders (" +
                "order_id STRING," +
                "amount DOUBLE)" +
                "WITH (" +
                "'connector' = 'kafka'," +
                "'topic' = 'topic_test'," +
                "'properties.bootstrap.servers' = '10.20.1.26:9092'," +
                "'format' = 'json'," +
                "'scan.startup.mode' = 'latest-offset'" +
                ")";
        tableEnv.executeSql(createKafkaSourceDDL);

        // 文件系统批处理表
        String createFilesystemSourceDDL = "CREATE TABLE file_batch_orders (" +
                "order_id STRING," +
                "amount DOUBLE)" +
                "WITH (" +
                "'connector' = 'filesystem'," +
                "'path' = 'file:///Users/yclxiao/Project/bigdata/flink-blog/doc/input_order.csv'," +
                "'format' = 'csv'" +
                ")";
        tableEnv.executeSql(createFilesystemSourceDDL);

        // 执行统一查询，计算总金额
        Table resultTable = tableEnv.sqlQuery("SELECT SUM(amount) FROM (" +
                "SELECT amount FROM kafka_stream_orders " +
                "UNION ALL " +
                "SELECT amount FROM file_batch_orders)");

        // 打印结果
        tableEnv.toRetractStream(resultTable, Row.class).print();

        // 开始执行程序
        env.execute("Stream-Batch Unified Job");
    }
}