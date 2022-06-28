package com.atguigu.chapter08_multistream;

import com.atguigu.chapter05_StreamAPI.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 测试Connect
 * @author malichun
 * @create 2022/6/26 23:32
 */
public class Test03_ConnectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Integer> stream1 = env.fromElements(1,2,3);

        SingleOutputStreamOperator<Long> stream2 = env.fromElements(4L, 5L, 6L, 7L);

       stream1.connect(stream2).map(new CoMapFunction<Integer, Long, String>() { //<IN1, IN2, OUT>
           @Override
           public String map1(Integer value) throws Exception {
               return value.toString();
           }

           @Override
           public String map2(Long value) throws Exception {
               return value.toString();
           }
       }).print();

        env.execute();
    }
}
