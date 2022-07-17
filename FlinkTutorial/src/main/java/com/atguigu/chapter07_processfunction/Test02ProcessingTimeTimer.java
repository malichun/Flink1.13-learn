package com.atguigu.chapter07_processfunction;

import com.atguigu.chapter05_StreamAPI.ClickSource;
import com.atguigu.chapter05_StreamAPI.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author malichun
 * @create 2022/6/26 18:41
 */
public class Test02ProcessingTimeTimer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() { // 泛型<K, I, O>
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        // 获取当前处理时间
                        Long currentTs = ctx.timerService().currentProcessingTime();
                        out.collect(ctx.getCurrentKey() +"数据到达时间: "+ new Timestamp(currentTs));

                        // 注册一个10秒后的定时器
                        ctx.timerService().registerProcessingTimeTimer(currentTs + 10 * 1000L);

                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 触发定时器
                        out.collect(ctx.getCurrentKey() + " 定时器触发, 触发时间: "+ new Timestamp(timestamp));
                    }
                }).print();

        env.execute();
    }
}
