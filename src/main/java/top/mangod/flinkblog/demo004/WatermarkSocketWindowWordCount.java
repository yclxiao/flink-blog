package top.mangod.flinkblog.demo004;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author: baily
 * @mail: yclxiao@gmail.com
 **/
public class WatermarkSocketWindowWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> textStream = env.socketTextStream("localhost", 9999, "\n");

        // 比如输入：1000,a   2000,a  3000,b
        DataStream<Tuple2<String, Integer>> windowCountStream = textStream
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
                .sum(1);

        windowCountStream.print(" ========== ").setParallelism(1);

        env.execute(WatermarkSocketWindowWordCount.class.getSimpleName());
    }
}
