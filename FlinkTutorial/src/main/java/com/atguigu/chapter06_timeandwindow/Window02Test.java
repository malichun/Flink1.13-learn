package com.atguigu.chapter06_timeandwindow;

import com.atguigu.chapter05_StreamAPI.ClickSource;
import com.atguigu.chapter05_StreamAPI.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author malichun
 * @create 2022/6/25 10:38
 */
public class Window02Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置watermark生成的周期
        env.getConfig().setAutoWatermarkInterval(200);

        //DataStream<Event> stream = env.fromElements(
        //        new Event("Mary", "./home", 1000L),
        //        new Event("Bob", "./cart", 2000L),
        //        new Event("Alice", "./prod?id=100", 3000L),
        //        new Event("Bob", "./prod?id=1", 3500L),
        //        new Event("Alice", "./prod?id=200", 3200L),
        //        new Event("Bob", "./home", 3300L),
        //        new Event("Bob", "./prod?id=2", 3800L),
        //        new Event("Bob", "./prod?id=3", 4200L)
        //    )
        DataStream<Event> stream = env.addSource(new ClickSource())
            // 有序流的watermark生成
            //.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
            //    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            //        @Override
            //        public long extractTimestamp(Event element, long recordTimestamp) {
            //            return element.timestamp; // 毫秒
            //        }
            //    })
            //)
            // 乱序流的watermark生成
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
            );

        // 窗口分配器
        stream
            .map(new MapFunction<Event, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(Event value) throws Exception {
                    return Tuple2.of(value.user, 1);
                }
            })
            .keyBy(t -> t.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 滚动事件时间窗口
            //.window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5))) // 滑动事件时间窗口
            //.window(EventTimeSessionWindows.withGap(Time.seconds(2))) // 事件时间会话窗口
            //    .countWindow(10, 2) //滑动计数窗口
            .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                    return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                }
            })
            .print();

        env.execute();
    }
}
