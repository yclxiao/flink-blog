package top.mangod.flinkblog.demo006sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSqlDemo {
    public static void main(String[] args) throws Exception {
        // 设置环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //为了方便测试看效果，这里并行度设置为1
        // 使用EnvironmentSettings创建StreamTableEnvironment，明确设置为批处理模式
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inBatchMode() // 设置为批处理模式，这样后续才能一次性的输出到csv中
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 定义输入数据源
        String createSourceTableDdl = "CREATE TABLE csv_source (" +
                " user_id INT," +
                " product STRING," +
                " order_amount DOUBLE" +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'file:///Users/yclxiao/Project/bigdata/flink-blog/doc/input.csv'," +
                " 'format' = 'csv'" +
                ")";
        tableEnv.executeSql(createSourceTableDdl);

//        // 编写 SQL 查询
//        String query = "SELECT user_id, SUM(order_amount) AS total_amount FROM csv_source GROUP BY user_id";
//        // 执行查询并打印
//        tableEnv.executeSql(query).print();
//        env.execute("Flink SQL Demo");

        // 定义输出数据源
        String createSinkTableDdl = "CREATE TABLE csv_sink (" +
                " user_id INT," +
                " total_amount DOUBLE" +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'file:///Users/yclxiao/Project/bigdata/flink-blog/doc/output.csv'," +
                " 'format' = 'csv'" +
                ")";
        tableEnv.executeSql(createSinkTableDdl);

        // 执行查询并将结果输出到csv_sink
        String query = "INSERT INTO csv_sink " +
                "SELECT user_id, SUM(order_amount) as total_amount " +
                "FROM csv_source " +
                "GROUP BY user_id";
        tableEnv.executeSql(query);

//        env.execute("Flink SQL Job");
    }
}
