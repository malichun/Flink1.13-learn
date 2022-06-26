package com.atguigu.chapter06;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;


/**
 * 增量聚合和全窗口函数的结合使用
 * @author malichun
 * @create 2022/6/25 17:32
 */
public class Window06UVCountExample {
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

        // 使用AggregateFunction 和 ProcessWindowFunction结合计算
        stream.keyBy(data -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
            .aggregate(new UvAgg(), new UvCountResult())
            .print()
        ;


        env.execute();
    }

    // 自定义实现AggregateFunction , 增量聚合计算UV值
    public static class UvAgg implements AggregateFunction<Event, HashSet<String>, Long>{ // K, agg, v

        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(Event value, HashSet<String> accumulator) {
            accumulator.add(value.user);
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            return (long)accumulator.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
            return null;
        }
    }

    // 自定义实现ProcessWindowFunction, 包装窗口信息输出(输入是上面getResult的输出)
    public static class UvCountResult extends ProcessWindowFunction<Long, String, Boolean, TimeWindow>{//IN, OUT, KEY, W extends Window

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Long, String, Boolean, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long uv = elements.iterator().next(); // 获取uv
            out.collect("窗口 "+ new Timestamp(start) + " ~ " + new Timestamp(end)+ " uv值为: " + uv);
        }

    }

}
