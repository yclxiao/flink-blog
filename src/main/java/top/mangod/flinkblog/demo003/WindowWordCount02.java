package top.mangod.flinkblog.demo003;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @author: baily
 * @mail: yclxiao@gmail.com
 * @created: 2023/7/10 16:07
 * 本文DEMO示例：每5分钟统计一次单词的数量
 **/
public class WindowWordCount02 {
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
                .window(GlobalWindows.create())
                // 清除器，用于清除超过evictionSec前的数据。防止整个窗口的数据量过大
                .evictor(TimeEvictor.of(Time.of(30, TimeUnit.SECONDS)))
                .trigger(new WindowWordCount02.MyTrigger(textStream.getType().createSerializer(env.getConfig())))
                // 对某个组里的单词的数量进行滚动相加统计
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> a, Tuple2<String, Integer> b) throws Exception {
                        return new Tuple2<>(a.f0, a.f1 + b.f1);
                    }
                });

        // 4. 数据输出。字节输出到控制台
        wordCountStream.print("WindowWordCount02 ======= ").setParallelism(1);
        // 5. 启动任务
        env.execute(WindowWordCount02.class.getSimpleName());
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

    private static class MyTrigger<T, W extends Window> extends Trigger<T, W> {
        private final ValueStateDescriptor<T> stateDesc;

        private MyTrigger(TypeSerializer<T> stateSerializer) {
            stateDesc = new ValueStateDescriptor<>("last-element", stateSerializer);
        }

        @Override
        public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception {
            Tuple2<String, Integer> elementValue = (Tuple2<String, Integer>) element;
            ValueState<T> lastElementState = ctx.getPartitionedState(stateDesc);
            if (lastElementState.value() == null) {
                lastElementState.update(element);
                return TriggerResult.CONTINUE;
            }
            // 此处状态描述器ValueState可以不使用
            Tuple2<String, Integer> lastValue = (Tuple2<String, Integer>) lastElementState.value();
            if (elementValue.f0.equals("1")) {
                lastElementState.update(element);
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(W window, TriggerContext ctx) throws Exception {
            ctx.getPartitionedState(stateDesc).clear();
        }
    }
}
