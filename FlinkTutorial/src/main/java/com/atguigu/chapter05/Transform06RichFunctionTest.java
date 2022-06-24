package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author malichun
 * @create 2022/6/23 0:16
 */
public class Transform06RichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        stream.map(new MyRichMapper()).print();

        env.execute();
    }

    public static class MyRichMapper extends RichMapFunction<Event, Integer>{

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open生命周期被调用: "+ getRuntimeContext().getIndexOfThisSubtask() + "号任务启动");
        }

        @Override
        public Integer map(Event value) throws Exception {
            return value.url.length();
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("open生命周期被调用: "+ getRuntimeContext().getIndexOfThisSubtask() + "号任务结束");
        }

    }
}
