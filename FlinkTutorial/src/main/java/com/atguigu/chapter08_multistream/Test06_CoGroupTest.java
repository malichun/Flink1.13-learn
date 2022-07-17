package com.atguigu.chapter08_multistream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
/**
 *
 * join会打散, coGroup 会一起拿过来, 想怎么处理就怎么处理
 * @author malichun
 * @create 2022/6/28 20:22
 */
public class Test06_CoGroupTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env.fromElements(
            Tuple2.of("a", 1000L),
            Tuple2.of("b", 1000L),
            Tuple2.of("a", 2000L),
            Tuple2.of("b", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
            .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                @Override
                public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                    return stringLongTuple2.f1;
                }
            }));

        SingleOutputStreamOperator<Tuple2<String, Long>> stream2 = env.fromElements(
            Tuple2.of("a", 3000L),
            Tuple2.of("b", 3000L),
            Tuple2.of("a", 4000L),
            Tuple2.of("b", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
            .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                @Override
                public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                    return stringLongTuple2.f1;
                }
            }));

        // 相同的key内的数据, 任何一方有, 都会输出
        stream1.coGroup(stream2)
            .where(data -> data.f0)
            .equalTo(data -> data.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .apply(new CoGroupFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {

                @Override
                public void coGroup(Iterable<Tuple2<String, Long>> iter1, Iterable<Tuple2<String, Long>> iter2, Collector<String> collector) throws Exception {
                    collector.collect(iter1 + " =   > " + iter2);
                }
            })

            .print();

        env.execute();
    }

}
