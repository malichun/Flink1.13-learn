package com.atguigu.chapter06_timeandwindow;

import com.atguigu.chapter05_StreamAPI.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 测试是水位线和窗口的使用
 *
 * Alice, ./home, 1000
 * Alice, ./cart, 2000
 * Alice, ./prod?id=100, 10000
 * Alice, ./prod?id=200, 8000
 * Alice, ./prod?id=300, 15000  # 这条数据会触发 [0-10)窗口的触发
 * Alice, ./prod?id=200, 9000   # 这条数据会被丢弃
 *
 * @author malichun
 * @create 2022/6/25 22:52
 */
public class WaterMark08Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 将数据源改为 socket 文本流，并转换成 Event 类型
        env.socketTextStream("hadoop102", 7777)
            .map(new MapFunction<String, Event>() {
                @Override
                public Event map(String value) throws Exception {
                    String[] fields = value.split(",");
                    return new Event(fields[0].trim(), fields[1].trim(),
                        Long.valueOf(fields[2].trim()));
                }
            })
            // 插入水位线的逻辑
            .assignTimestampsAndWatermarks(
                // 针对乱序流插入水位线，延迟时间设置为 5s

                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                        // 抽取时间戳的逻辑
                        @Override
                        public long extractTimestamp(Event element, long
                            recordTimestamp) {
                            return element.timestamp;
                        }
                    })
            )
            // 根据 user 分组，开窗统计
            .keyBy(data -> data.user)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .process(new WatermarkTestResult())
            .print();

        env.execute();

    }

    // 自定义处理窗口函数，输出当前的水位线和窗口信息
    public static class WatermarkTestResult extends ProcessWindowFunction<Event, String, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            Long currentWatermark = context.currentWatermark();
            Long count = elements.spliterator().getExactSizeIfKnown();
            out.collect("窗口" + start + " ~ " + end + "中共有" + count + "个元素，窗口闭合计算时，水位线处于：" + currentWatermark);
        }
    }
}
