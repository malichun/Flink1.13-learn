package com.atguigu.chapter05_StreamAPI;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author malichun
 * @create 2022/6/22 20:46
 */
public class Transform01MapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 进行转换计算, 提取user字段
        // 1. 使用自定义类实现MapFunction接口
        SingleOutputStreamOperator<String> result = stream.map(new MyMapper());

        // 2. 使用匿名类实现MapFunction接口
        SingleOutputStreamOperator<String> result2 = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.user;
            }
        });

        // 3.传入lambda表达式
        SingleOutputStreamOperator<String> result3 = stream.map(event -> event.user);


        result3.print();

        env.execute();
    }

    public static class MyMapper implements MapFunction<Event, String>{

        @Override
        public String map(Event value) throws Exception {
            return value.user;
        }
    }
}
