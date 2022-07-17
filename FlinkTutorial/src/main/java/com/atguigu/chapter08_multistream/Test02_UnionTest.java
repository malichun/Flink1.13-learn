package com.atguigu.chapter08_multistream;

import com.atguigu.chapter05_StreamAPI.ClickSource;
import com.atguigu.chapter05_StreamAPI.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author malichun
 * @create 2022/6/26 23:32
 */
public class Test02_UnionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream1 = env.socketTextStream("hadoop102", 7777)
            .map(data -> {
                String[] field = data.split(",");
                return new Event(field[0].trim(), field[1].trim(), Long.valueOf(field[2].trim()));
            })
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));


        SingleOutputStreamOperator<Event> stream2 = env.socketTextStream("hadoop103", 7777)
            .map(data -> {
                String[] field = data.split(",");
                return new Event(field[0].trim(), field[1].trim(), Long.valueOf(field[2].trim()));
            })
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        stream1.print("stream1");
        stream2.print("stream2");

        stream1.union(stream2)
            .process(new ProcessFunction<Event, String>() {
                @Override
                public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                    out.collect("水位线" + ctx.timerService().currentWatermark());
                }
            }).print();


        env.execute();
    }
}
