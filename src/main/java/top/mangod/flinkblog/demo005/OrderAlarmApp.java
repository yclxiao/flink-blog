package top.mangod.flinkblog.demo005;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author: baily
 * @mail: yclxiao@gmail.com
 * @created: 2023/5/10 19:48
 * <p>
 * 模拟场景：某个用户1分钟内连续两次退款，第二次发出告警。
 * 示例数据：
 * 1,aaa,100,1,user1
 * 2,bbb,200,1,user2
 * 3,ccc,300,2,user1
 * 4,ddd,400,2,user1
 * <p>
 * 5,ddd,400,2,user1
 * 6,bbb,200,2,user2
 * 7,bbb,400,2,user2
 **/
public class OrderAlarmApp {
    public static void main(String[] args) throws Exception {
        // 1、执行环境创建
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2、读取Socket数据端口。实际根据具体业务对接数据来源
        DataStreamSource<String> orderStream = environment.socketTextStream("localhost", 9527);
        // 3、数据读取个切割方式
        SingleOutputStreamOperator<OrderBO> resultDataStream = orderStream
                .flatMap(new CleanDataAnd2Order()) // 清洗和处理数据
                .keyBy(x -> x.getUserId()) // 分区
                .process(new AlarmLogic()); // 处理告警逻辑

        // 4、打印分析结果
        resultDataStream.print("告警===>");
        // 5、环境启动
        environment.execute("OrderAlarmApp");
    }

    /**
     * 清洗流入的数据，再转换成订单对象
     */
    private static class CleanDataAnd2Order implements FlatMapFunction<String, OrderBO> {
        @Override
        public void flatMap(String input, Collector<OrderBO> collector) throws Exception {
            // 简单的数据清洗
            if (StringUtils.isBlank(input)) {
                return;
            }
            String[] wordArr = input.split(",");
            if (wordArr.length < 5) {
                return;
            }
            // 转成需要计算的对象
            OrderBO orderBO = new OrderBO();
            orderBO.setId(Integer.valueOf(wordArr[0]));
            orderBO.setTitle(wordArr[1]);
            orderBO.setAmount(Integer.valueOf(wordArr[2]));
            orderBO.setState(Integer.valueOf(wordArr[3]));
            orderBO.setUserId(wordArr[4]);
            // 发射给下一个算子
            collector.collect(orderBO);
        }
    }

    /**
     * 处理流入的数据，做告警逻辑，1分钟内如果出现2次退款则发出告警
     * 入参：key  输入的数据   输出的数据
     */
    private static class AlarmLogic extends KeyedProcessFunction<String, OrderBO, OrderBO> {
        // 是否已经出现退款的标记
        private ValueState<Boolean> flagState;
        // 定时器，时间到了会清掉状态
        private ValueState<Long> timerState;
        private static final long ONE_MINUTE = 60 * 1000;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                    "flag",
                    Types.BOOLEAN);
            flagState = getRuntimeContext().getState(flagDescriptor);

            ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                    "timer-state",
                    Types.LONG);
            timerState = getRuntimeContext().getState(timerDescriptor);
        }

        @Override
        public void processElement(OrderBO value, KeyedProcessFunction<String, OrderBO, OrderBO>.Context context, Collector<OrderBO> collector) throws Exception {
            Boolean refundFlag = flagState.value();

            // 如果已经退款过一次了，如果再出现退款则发射给下个算子，然后清理掉定时器。状态2代表退款
            if (refundFlag != null && refundFlag) {
                if (value.getState() == 2) {
                    collector.collect(value);
                }
                cleanUp(context);
            } else {
                // 如果第一次出现退款，则写入状态，同时开启定时器。状态2代表退款
                if (value.getState() == 2) {
                    flagState.update(true);
                    long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
                    context.timerService().registerProcessingTimeTimer(timer);
                    timerState.update(timer);
                }
            }
        }

        /**
         * 定时器到了之后，清理状态值
         */
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, OrderBO, OrderBO>.OnTimerContext ctx, Collector<OrderBO> out) throws Exception {
            timerState.clear();
            flagState.clear();
        }

        /**
         * 手动清理状态值
         *
         * @param ctx
         * @throws Exception
         */
        private void cleanUp(Context ctx) throws Exception {
            Long timer = timerState.value();
            ctx.timerService().deleteProcessingTimeTimer(timer);

            timerState.clear();
            flagState.clear();
        }
    }
}
