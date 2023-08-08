package top.mangod.flinkblog.demo004;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author: baily
 * @mail: yclxiao@gmail.com
 **/
public class LateEventsSocketWindowWordCount {
    private static final OutputTag<Tuple2<String, Integer>> lateEventsTag =
            new OutputTag<Tuple2<String, Integer>>("late-events") {
            };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> textStream = env.socketTextStream("localhost", 9999, "\n");

        // 比如输入：1000,a   2000,a  3000,b
        SingleOutputStreamOperator<Tuple2<String, Integer>> windowCountStream = textStream
                // 水印策略，对于过来的事件时间上，可以延迟2秒
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((event, timestamp) ->
                                        Long.parseLong(event.split(",")[0])))
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] splits = value.split(",");
                        return Tuple2.of(splits[1], 1);
                    }
                })
                .keyBy(value -> value.f0)
                // 滚动5分钟的窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(lateEventsTag)
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(input.iterator().next());
                    }
                });

        // 正常输出的元素
        windowCountStream.print("正常数据 ========== ").setParallelism(1);

        // 输出迟到的元素
        DataStream<Tuple2<String, Integer>> lateEvents = windowCountStream.getSideOutput(lateEventsTag);
        lateEvents.print("延迟侧道数据 ========== ").setParallelism(1);

        env.execute(LateEventsSocketWindowWordCount.class.getSimpleName());
    }
}
