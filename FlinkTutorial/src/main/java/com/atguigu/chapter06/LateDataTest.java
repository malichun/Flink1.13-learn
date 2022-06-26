package com.atguigu.chapter06;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 测试迟到数据
 * @author malichun
 * @create 2022/6/26 16:20
 */
public class LateDataTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
        // 乱序流的watermark
        SingleOutputStreamOperator<Event> stream = env
            .socketTextStream("hadoop102",9999)
            .map(new MapFunction<String, Event>() {
                @Override
                public Event map(String value) throws Exception {
                    String[] arr = value.split(",");
                    return new Event(arr[0].trim(), arr[1].trim(), Long.parseLong(arr[2].trim()));
                }
            })
            //乱序刘的watermark生成
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
            );

        stream.print("input");

        // 定义一个输出标签
        OutputTag<Event> late = new OutputTag<Event>("late") {};

        // 统计每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> result = stream.keyBy(event -> event.url)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .allowedLateness(Time.seconds(10))
            .sideOutputLateData(late)
            .aggregate(new UrlViewCountAgg(), new UrlViewCountResult())
            ;
        result.getSideOutput(late).print("late");

        result.print("result");

        env.execute();
    }

    // 增量聚合, 来一条数据就+1
    public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    // 包装信息, 输出 <IN, OUT, KEY, W extends Window>
    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String url, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long count = elements.iterator().next();
            out.collect(new UrlViewCount(url, count, start, end));
        }
    }
}
