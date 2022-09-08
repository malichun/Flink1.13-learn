package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author malichun
 * @create 2022/09/07 0007 23:05
 */
public class _21_WindowApiDemo3 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // 1,e01,3000,pg02
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<EventBean2, Integer>> beanStream = source.map(s -> {
                String[] split = s.split(",");
                EventBean2 eventBean2 = new EventBean2(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3], Integer.parseInt(split[4]));
                return Tuple2.of(eventBean2,1);
            }).returns(new TypeHint<Tuple2<EventBean2, Integer>>() {})
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<EventBean2, Integer>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<EventBean2, Integer>>() {
                    @Override
                    public long extractTimestamp(Tuple2<EventBean2, Integer> element, long recordTimestamp) {
                        return element.f0.getTimeStamp();
                    }
                }));

        OutputTag<Tuple2<EventBean2, Integer>> lateTag = new OutputTag<Tuple2<EventBean2, Integer>>("late", TypeInformation.of(new TypeHint<Tuple2<EventBean2, Integer>>() {}));

        SingleOutputStreamOperator<Tuple2<EventBean2, Integer>> sumResult = beanStream.keyBy(x -> x.f0.getGuid())
            .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 事件时间滚动窗口
            .allowedLateness(Time.seconds(2)) // 允许迟到2秒
            .sideOutputLateData(lateTag) // 迟到超过允许时限的数据, 输出到该"outputtag"所标记的测流
            .sum(1);

        sumResult.print("main");

        DataStream<Tuple2<EventBean2, Integer>> lateData = sumResult.getSideOutput(lateTag);
        lateData.print("late");


        env.execute();
    }
}
