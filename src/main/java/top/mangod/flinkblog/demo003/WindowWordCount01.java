package top.mangod.flinkblog.demo003;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.util.Collector;

/**
 * @author: baily
 * @mail: yclxiao@gmail.com
 * @created: 2023/7/10 16:07
 * 本文DEMO示例：每5分钟统计一次单词的数量
 **/
public class WindowWordCount01 {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据源
        DataStream<String> textStream = env.socketTextStream("localhost", 9999, "\n");

        // 3. 数据转换
        DataStream<Tuple2<String, Integer>> wordCountStream = textStream
                .assignTimestampsAndWatermarks(MyWatermark.create())
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
                // 基于事件时间的滑动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 基于处理时间的滑动窗口
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                // 对某个组里的单词的数量进行滚动相加统计
                .reduce((a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1));

        // 4. 数据输出。字节输出到控制台
        wordCountStream.print("WindowWordCount01 ======= ").setParallelism(1);
        // 5. 启动任务
        env.execute(WindowWordCount01.class.getSimpleName());
    }

    private static class MyWatermark<T> implements WatermarkStrategy<T> {

        private MyWatermark() {
        }

        public static <T> MyWatermark<T> create() {
            return new MyWatermark<>();
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> System.currentTimeMillis();
        }
    }
}
