package com.atguigu.chapter05_StreamAPI;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author malichun
 * @create 2022/6/22 20:46
 */
public class Transform04SimpleAggTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3500L),
                new Event("Bob", "./home", 3300L),
                new Event("Alice", "./prod?id=200", 3200L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Bob", "./prod?id=3", 4200L)
        );

        // 按键分区之后进行聚合, 提取当前用户最近一次访问数据
        KeyedStream<Event, String> keyedStream = stream.keyBy(e -> e.user);


        keyedStream.max("timestamp")
                        .print("max: ");

        keyedStream.maxBy("timestamp")
                .print("max: ");



        env.execute();
    }

}
