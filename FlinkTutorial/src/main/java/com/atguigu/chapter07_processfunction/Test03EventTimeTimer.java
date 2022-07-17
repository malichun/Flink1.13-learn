package com.atguigu.chapter07_processfunction;

import com.atguigu.chapter05_StreamAPI.ClickSource;
import com.atguigu.chapter05_StreamAPI.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author malichun
 * @create 2022/6/26 18:52
 */
public class Test03EventTimeTimer {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                        @Override
                        public long extractTimestamp(Event element, long recordTimestamp) {
                            return element.timestamp;
                        }
                    })
                );

        // 事件时间定时器
        stream.keyBy(data -> data.user)
            .process(new KeyedProcessFunction<String, Event, String>() { // 泛型<K, I, O>
                @Override
                public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                    // 获取当前处理时间
                    Long currentTs = ctx.timestamp();
                    out.collect(ctx.getCurrentKey() +" 数据到达, 时间戳: "+ new Timestamp(currentTs) +" 当前watermark: "+ctx.timerService().currentWatermark());

                    // 注册一个10秒后的定时器
                    ctx.timerService().registerEventTimeTimer(currentTs + 10 * 1000L);

                }

                @Override
                public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                    // 触发定时器
                    out.collect(ctx.getCurrentKey() + " 定时器触发, 触发时间: "+ new Timestamp(timestamp) + " watermark: "+ ctx.timerService().currentWatermark());
                }
            }).print();

        env.execute();
    }

    // 自定义数据源
    public static class CustomSource implements SourceFunction<Event>{
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            // 直接发出测试数据
            ctx.collect(new Event("Mary", "./home", 1000L));
            Thread.sleep(5000);

            ctx.collect(new Event("Alice", "./home", 11000L));
            Thread.sleep(5000);

            ctx.collect(new Event("Bob", "./home", 11001L));
            Thread.sleep(5000);
        }

        @Override
        public void cancel() {

        }
    }

}
