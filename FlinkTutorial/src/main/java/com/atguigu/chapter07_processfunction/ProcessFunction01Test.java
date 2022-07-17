package com.atguigu.chapter07_processfunction;

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
 * @create 2022/6/26 17:35
 */
public class ProcessFunction01Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        stream.process(new ProcessFunction<Event, String>() { // <I, O>

            // 每来一条数据都会调用一次
            @Override
            public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                if(value.user.equals("Mary")){
                    out.collect(value.user + " clicks "+ value.url);
                }else if(value.user.equals("Bob")){
                    out.collect(value.user);
                    out.collect(value.user);
                }

                out.collect(value.toString());
                System.out.println("timestamp: " +ctx.timestamp());
                System.out.println("watermark"+ctx.timerService().currentProcessingTime());


                System.out.println(getRuntimeContext().getIndexOfThisSubtask());
                //getRuntimeContext().getState() // 自定义状态
            }

        }).print();

        env.execute();
    }
}
