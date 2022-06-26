package com.atguigu.chapter06_timeandwindow;

import com.atguigu.chapter05_StreamAPI.ClickSource;
import com.atguigu.chapter05_StreamAPI.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;


/**
 * 全窗口 计算UV
 *
 * @author malichun
 * @create 2022/6/25 17:32
 */
public class Window05ProcessWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
        // 乱序流的watermark
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
            );

        stream.print();

        // 使用ProcessWindowFunction计算UV
        stream.keyBy(data -> true)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .process(new UvCountByWindow())
            .print();


        env.execute();
    }

    // 实现自定义的ProcessWindowFunction, 输出一条统计信息
    // 也是抽象类
    public static class UvCountByWindow extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> { // IN, OUT, KEY , WINDOW

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            // 用一个HashSet保存user
            HashSet<String> userSet = new HashSet<>();
            // 从element中遍历数据,放到Set中去重
            for(Event event: elements){
                userSet.add(event.user);
            }
            int uv = userSet.size();
            // 结合窗口信息输出
            long start = context.window().getStart();
            long end = context.window().getEnd();
            out.collect("窗口 "+ new Timestamp(start) + " ~ " + new Timestamp(end)+ " uv值为: " + uv);
        }
    }

}
