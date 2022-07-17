package com.atguigu.chapter09_state;

import com.atguigu.chapter05_StreamAPI.ClickSource;
import com.atguigu.chapter05_StreamAPI.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 映射状态的用法和 Java 中的 HashMap 很相似。在这里我们可以通过 MapState 的使用来探
 * 索一下窗口的底层实现，也就是我们要用映射状态来完整模拟窗口的功能。这里我们模拟一个
 * 滚动窗口。我们要计算的是每一个 url 在每一个窗口中的 pv 数据。我们之前使用增量聚合和
 * 全窗口聚合结合的方式实现过这个需求。
 * <p>
 * // 使用 KeyedProcessFunction 模拟滚动窗口
 *
 * @author malichun
 * @create 2022/6/29 10:40
 */
public class Test04_MapState_FakeWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        })
                );

        // 统计每10s窗口内, 每个url的pv
        stream.keyBy(data -> data.url)
                .process(new FakeWindowResult(10000L))
                .print();


        env.execute();
    }

    public static class FakeWindowResult extends KeyedProcessFunction<String, Event, String>{ // K,I,O
        // 定义属性, 窗口长度
        private Long windowSize;

        public FakeWindowResult(Long windowSize){
            this.windowSize = windowSize;
        }

        // 声明状态, 用map保存pv值(窗口start, count)
        MapState<Long, Long> windowPvMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            windowPvMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-pv",Long.class, Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            //每来一条数据, 就根据时间戳判断属于哪个窗口
            Long windowStart = value.timestamp / windowSize * windowSize;
            long windowEnd = windowStart + windowSize;

            // 注册end-1的定时器, 窗口触发计算
            ctx.timerService().registerEventTimeTimer(windowEnd -1);

            // 更新状态中的pv值
            if(windowPvMapState.contains(windowStart)){
                Long pv = windowPvMapState.get(windowStart);
                windowPvMapState.put(windowStart, pv + 1);
            }else{
                windowPvMapState.put(windowStart, 1L);
            }
        }

        // 定时器触发, 直接输出统计的pv结果
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            Long windowEnd = timestamp + 1;
            Long windowStart = windowEnd -windowSize;

            Long pv = windowPvMapState.get(windowStart);

            out.collect("url" + ctx.getCurrentKey() + " 访问量: " + pv + " 窗口: " + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd));

            // 模拟窗口的销毁
            windowPvMapState.remove(windowStart);
        }
    }
}
