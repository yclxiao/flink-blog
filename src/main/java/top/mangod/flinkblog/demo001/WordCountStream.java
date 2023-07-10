package top.mangod.flinkblog.demo001;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: baily
 * @mail: yclxiao@gmail.com
 * @created: 2023/7/10 16:07
 * 本文DEMO示例-流计算：
 * 读取socket数据源，对输入的数据进行统计，最后输出到控制台
 * 执行main方法前，现在本地开启netcat，nc -lk 9999，然后输入任意字符，即可看到统计结果
 **/
public class WordCountStream {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据源
        DataStream<String> textStream = env.socketTextStream("localhost", 9999, "\n");
        // 3. 数据转换
        DataStream<Tuple2<String, Integer>> wordCountStream = textStream
                // 对数据源的单词进行拆分，每个单词记为1，然后通过out.collect将数据发射到下游算子
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                             @Override
                             public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                                 for (String word : value.split("\\s")) {
                                     out.collect(new Tuple2<>(word, 1));
                                 }
                             }
                         }
                )
                // 对单词进行分组
                .keyBy(value -> value.f0)
                // 对某个组里的单词的数量进行滚动相加统计
                .reduce((a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1));
        // 4. 数据输出。字节输出到控制台
        wordCountStream.print("WordCountStream=======").setParallelism(1);
        // 5. 启动任务
        env.execute(WordCountStream.class.getSimpleName());
    }
}
