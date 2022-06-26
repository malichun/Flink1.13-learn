package com.atguigu.chapter06;

import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author malichun
 * @create 2022/6/25 10:38
 */
public class WaterMark01Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置watermark生成的周期
        env.getConfig().setAutoWatermarkInterval(100);

        DataStream<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3500L),
                new Event("Alice", "./prod?id=200", 3200L),
                new Event("Bob", "./home", 3300L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Bob", "./prod?id=3", 4200L)
            )
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
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
            );



        env.execute();
    }
}
