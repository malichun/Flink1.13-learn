package com.atguigu.chapter08_multistream;

import com.atguigu.chapter05_StreamAPI.ClickSource;
import com.atguigu.chapter05_StreamAPI.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 测试分流
 * @author malichun
 * @create 2022/6/26 23:20
 */
public class Test01_SplitStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        // 定义输出标签
        OutputTag<Tuple3<String, String, Long>> maryTag = new OutputTag<Tuple3<String, String, Long>>("Mary") {};
        OutputTag<Tuple3<String, String, Long>> bobTag = new OutputTag<Tuple3<String, String, Long>>("Bob") {};

        // 使用processFunction
        SingleOutputStreamOperator<Event> processedStream = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                if (value.user.equals("Mary")) {
                    ctx.output(maryTag, Tuple3.of(value.user, value.url, value.timestamp));
                } else if (value.user.equals("Bob")) {
                    ctx.output(bobTag, Tuple3.of(value.user, value.url, value.timestamp));
                } else {
                    out.collect(value);
                }
            }
        });

        processedStream.print("else");

        processedStream.getSideOutput(maryTag).print("Mary");

        processedStream.getSideOutput(bobTag).print("Bob");

        env.execute();

    }
}
