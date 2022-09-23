package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author malichun
 * @create 2022/09/07 0007 23:05
 */
public class _21_WindowApiDemo2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // 1,e01,3000,pg02
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<EventBean2> beanStream = source.map(s -> {
                String[] split = s.split(",");
                return new EventBean2(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3], Integer.parseInt(split[4]));
            }).returns(EventBean2.class)
            .assignTimestampsAndWatermarks(WatermarkStrategy.<EventBean2>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean2>() {
                    @Override
                    public long extractTimestamp(EventBean2 element, long recordTimestamp) {
                        return element.getTimeStamp();
                    }
                }));

        /**
         * 各种全局窗口API
         */

        // 全局 计数滚动窗口
        beanStream.countWindowAll(10) // 10条数据一个窗口
            .apply(new AllWindowFunction<EventBean2, String, GlobalWindow>() {
                @Override
                public void apply(GlobalWindow window, Iterable<EventBean2> values, Collector<String> out) throws Exception {

                }
            });
        // 全局 计数滑动窗口
        beanStream.countWindowAll(10, 5);
        //.apply()

        // 全局 事件时间滚动窗口
        beanStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
            .apply(new AllWindowFunction<EventBean2, String, TimeWindow>() {
                @Override
                public void apply(TimeWindow window, Iterable<EventBean2> values, Collector<String> out) throws Exception {

                }
            });
        // 全局 事件时间滑动窗口
        beanStream.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
        /* .process() */;
        // 全局 事件时间会话窗口
        beanStream.windowAll(EventTimeSessionWindows.withGap(Time.seconds(30))) // 前后两个事件的间隙超过30s就划分窗口
        /* .process() */;

        // 全局 处理时间滚动窗口
        beanStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)));
        // 全局 处理时间滑动窗口
        beanStream.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(15)));
        // 全局 处理时间会话窗口
        beanStream.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(30)));

        /**
         * 2.各种keyedWindow开窗API
         */
        KeyedStream<EventBean2, Long> keyedStream = beanStream.keyBy(EventBean2::getGuid);
        // keyed 计数滚动窗口
        keyedStream.countWindow(10);

        // keyed 计数滑动窗口
        keyedStream.countWindow(10, 5);

        // keyed 事件时间滚动窗口
        keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .process(new ProcessWindowFunction<EventBean2, String, Long, TimeWindow>() {
                @Override
                public void process(Long aLong, ProcessWindowFunction<EventBean2, String, Long, TimeWindow>.Context context, Iterable<EventBean2> elements, Collector<String> out) throws Exception {

                }
            });

        // keyed 事件时间滑动窗口
        keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)));
        // keyed 事件时间会话窗口
        keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(30)));

        // keyed 处理时间滚动窗口
        keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        // keyed 处理时间滑动窗口
        keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)));
        // keyed 处理时间会话窗口
        keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)));


        env.execute();
    }
}
