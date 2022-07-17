package com.atguigu.chapter05_StreamAPI;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author malichun
 * @create 2022/6/22 20:46
 */
public class Transform03FlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        stream.flatMap((FlatMapFunction<Event, String>) (value, out) -> {
            if (value.user.equals("Mary")) {
                out.collect(value.user);
            } else {
                out.collect(value.user);
                out.collect(value.url);
            }
        }).returns(new TypeHint<String>() {})
                .print();


        env.execute();
    }

}
