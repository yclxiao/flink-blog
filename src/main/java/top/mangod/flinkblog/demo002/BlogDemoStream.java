package top.mangod.flinkblog.demo002;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author: baily
 * @mail: yclxiao@gmail.com
 * @created: 2023/7/10 16:07
 **/
public class BlogDemoStream {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据源
//        DataStream<String> textStream =
//                env.readTextFile("/Users/yclxiao/Project/bigdata/flink-blog/doc/words.txt");
        DataStreamSource<String> textStream = env.fromCollection(Arrays.asList(
                "java,c++,php,java,spring",
                "hadoop,scala",
                "c++,jvm,html,php"
        ));
        // 3. 数据转换
        DataStream<Tuple2<String, Integer>> dataStream = textStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String word : value.split("\\,")) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                .keyBy(value -> value.f0)
                .reduce((a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1));

        dataStream.print("BlogDemoStream=======")
                .setParallelism(1);

        // 5. 启动任务
        env.execute(BlogDemoStream.class.getSimpleName());
    }
}
