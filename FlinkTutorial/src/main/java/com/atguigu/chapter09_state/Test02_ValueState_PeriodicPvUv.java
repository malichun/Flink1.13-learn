package com.atguigu.chapter09_state;

import com.atguigu.chapter05_StreamAPI.ClickSource;
import com.atguigu.chapter05_StreamAPI.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 我们这里会使用用户 id 来进行分流，然后分别统计每个用户的 pv 数据，由于我们并不想
 * 每次 pv 加一，就将统计结果发送到下游去，所以这里我们注册了一个定时器，用来隔一段时
 * 间发送 pv 的统计结果，这样对下游算子的压力不至于太大。具体实现方式是定义一个用来保
 * 存定时器时间戳的值状态变量。当定时器触发并向下游发送数据以后，便清空储存定时器时间
 * 戳的状态变量，这样当新的数据到来时，发现并没有定时器存在，就可以注册新的定时器了，
 * 注册完定时器之后将定时器的时间戳继续保存在状态变量中。
 * <p>
 * 周期性生成pv数据
 *
 * @author malichun
 * @create 2022/6/29 0:57
 */
public class Test02_ValueState_PeriodicPvUv {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        stream.print();

        // 统计每个用户的pv
        stream.keyBy(data -> data.user)
            .process(new PeriodicPvResult())
            .print();

        env.execute();
    }

    // 实现自定义的KeyedProcessFunction
    public static class PeriodicPvResult extends KeyedProcessFunction<String, Event, String>{

        // 定义状态, 保存当前pv统计值, 以及有没有定时器
        ValueState<Long> countState;
        // 定时器的状态
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            // 没来一条数据, 就更新对应的count值
            countState.update(countState.value() == null ? 1 : countState.value()+1);

            // 如果没有注册过的话, 注册定时器
            if(timerTsState.value() == null){
                timerTsState.update(value.timestamp);
                ctx.timerService().registerEventTimeTimer(value.timestamp + 10000L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发, 输出一次统计结果
            out.collect(ctx.getCurrentKey() + " pv: " + (countState.value() == null? 0: countState.value()));
            // 时间状态清空
            timerTsState.clear();
        }
    }
}
