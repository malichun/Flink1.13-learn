package com.atguigu.chapter06;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author malichun
 * @create 2022/6/25 17:18
 */
public class Window03AggregateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
            );

        stream.keyBy(data -> data.user)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            // 功能: 计算平均时间戳
            .aggregate(new AggregateFunction<Event, Tuple2<Long, Integer>, String>() { //IN, ACC, OUT
                // 初始值
                @Override
                public Tuple2<Long, Integer> createAccumulator() {
                    return Tuple2.of(0L, 0);
                }

                // 状态的叠加
                @Override
                public Tuple2<Long, Integer> add(Event value, Tuple2<Long, Integer> accumulator) {
                    return Tuple2.of(accumulator.f0 + value.timestamp, accumulator.f1 + 1);
                }

                // 获取结果
                @Override
                public String getResult(Tuple2<Long, Integer> accumulator) {
                    Timestamp timestamp = new Timestamp(accumulator.f0 / accumulator.f1);
                    return timestamp.toString();
                }

                // 合并两个累加器(在会话窗口中会用到)
                @Override
                public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
                    return Tuple2.of(a.f0+b.f0, a.f1+b.f1);
                }
            })
                .print();

        env.execute();
    }
}
