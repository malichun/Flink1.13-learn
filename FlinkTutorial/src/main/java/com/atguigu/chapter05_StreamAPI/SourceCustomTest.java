package com.atguigu.chapter05_StreamAPI;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author malichun
 * @create 2022/6/19 23:46
 */
public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> customStream = env.addSource(new ClickSource());

        customStream.print();

        env.execute();
    }
}
