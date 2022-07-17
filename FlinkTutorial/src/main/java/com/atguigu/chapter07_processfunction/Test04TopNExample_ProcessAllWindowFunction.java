package com.atguigu.chapter07_processfunction;

import com.atguigu.chapter05_StreamAPI.ClickSource;
import com.atguigu.chapter05_StreamAPI.Event;
import com.atguigu.chapter06_timeandwindow.UrlViewCount;
import com.atguigu.chapter06_timeandwindow.Window07UrlCountViewExample;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

/**
 * 计算TOPN, 全窗口函数
 *
 * @author malichun
 * @create 2022/6/26 20:41
 */
public class Test04TopNExample_ProcessAllWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        // 直接开窗, 收集所有数据
        stream
            .map(data -> data.url)
            .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))) // 定义滑动窗口
            .aggregate(new UrlHashMapCountAgg(), new UrlAllWindowResult())
            .print();


        env.execute();
    }

    // 实现自定义的增量聚合函数 泛型  <IN, ACC, OUT>
    public static class UrlHashMapCountAgg implements AggregateFunction<String, HashMap<String, Long>, ArrayList<Tuple2<String, Long>>>{

        @Override
        public HashMap<String, Long> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Long> add(String value, HashMap<String, Long> accumulator) {
            accumulator.put(value, accumulator.getOrDefault(value,0L)+1L);
            return accumulator;
        }

        @Override
        public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> accumulator) {
            ArrayList<Tuple2<String, Long>> result =
                (ArrayList<Tuple2<String,Long>>)accumulator.entrySet().stream().map(entry -> Tuple2.of(entry.getKey(), entry.getValue())).collect(Collectors.toList());
            result.sort((o1, o2) -> o2.f1.compareTo(o1.f1));
            return result;
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> a, HashMap<String, Long> b) {
            return null;
        }
    }

    // 实现自定义全窗口函数, 包装信息输出结果 <IN, OUT, W extends Window>
    public static class UrlAllWindowResult extends ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow> {

        @Override
        public void process(ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow>.Context context, Iterable<ArrayList<Tuple2<String, Long>>> elements, Collector<String> out) throws Exception {
            ArrayList<Tuple2<String, Long>> list = elements.iterator().next();
            StringBuilder result = new StringBuilder();
            result.append("---------------------------\n");
            result.append("窗口结束时间: " + new Timestamp(context.window().getEnd()) + "\n");

            // 取List前两个包装信息输出
            for (int i = 0; i < 2; i++) {
                Tuple2<String, Long> currentTuple = list.get(i);
                java.lang.String info = "No. " + (i + 1) + "  "
                    + "url:" + currentTuple.f0 + " "
                    + "访问量: " + currentTuple.f1 + "\n";
                result.append(info);
            }
            result.append("---------------------------\n");
            out.collect(result.toString());
        }
    }

}
