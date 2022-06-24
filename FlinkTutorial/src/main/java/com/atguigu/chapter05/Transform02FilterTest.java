package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author malichun
 * @create 2022/6/22 20:46
 */
public class Transform02FilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 1. 传入一个实现了FilterFunction的类对象
        SingleOutputStreamOperator<Event> result = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("Mary");
            }
        });

        // 1. 传入lambda表达式
        SingleOutputStreamOperator<Event> result2 = stream.filter(e -> e.user.equals(""));

        result2.print();

        env.execute();
    }

    public static class MyFilter implements FilterFunction<Event>{

        @Override
        public boolean filter(Event value) throws Exception {
            return value.user.equals("Mary");
        }
    }
}
