package com.atguigu.chapter07_processfunction;

import com.atguigu.chapter05_StreamAPI.ClickSource;
import com.atguigu.chapter05_StreamAPI.Event;
import com.atguigu.chapter06_timeandwindow.UrlViewCount;
import com.atguigu.chapter06_timeandwindow.Window07UrlCountViewExample;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;

/**
 * 计算TOPN
 * 使用定时器
 *
 * @author malichun
 * @create 2022/6/26 20:41
 */
public class Test05TopNExample_KeyedProcessWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        // 1.按照url分组, 统计窗口内每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(data -> data.url)
            .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))) // 定义滑动窗口
            .aggregate(new Window07UrlCountViewExample.UrlViewCountAgg(), new Window07UrlCountViewExample.UrlViewCountResult());

        //urlCountStream.print("url count");

        // 2. 对于同一窗口统计出的访问量进行收集和排序
        urlCountStream.keyBy(urlViewCount -> urlViewCount.windowEnd)
            .process(new TopNProcessResult(2))
            .print();


        env.execute();
    }

    // 实现自定义的KeyedProcessFunction
    static class TopNProcessResult extends KeyedProcessFunction<Long, UrlViewCount, String> { //<K,I,O>
        // 定义一个属性
        private Integer n;
        // 定义列表状态
        private ListState<UrlViewCount> urlViewCountListState;

        public TopNProcessResult(int topN) {
            this.n = topN;
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            // 在环境中获取状态
            urlViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("url-count-list", Types.POJO(UrlViewCount.class)));
        }

        @Override
        public void processElement(UrlViewCount value, KeyedProcessFunction<Long, UrlViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            // 将数据保存到状态中
            urlViewCountListState.add(value);
            // 注册一个定时器 windowEnd + 1ms的定时器
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1L);
        }


        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();

            for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
                urlViewCountArrayList.add(urlViewCount);
            }

            // 排序
            urlViewCountArrayList.sort((o1, o2) -> o2.count.compareTo(o1.count));

            // 包装信息打印输出
            StringBuilder result = new StringBuilder();
            result.append("---------------------------\n");
            result.append("窗口结束时间: " + new Timestamp(ctx.getCurrentKey()) + "\n");

            // 取List前两个包装信息输出
            for (int i = 0; i < 2; i++) {
                UrlViewCount urlViewCount = urlViewCountArrayList.get(i);
                java.lang.String info = "No. " + (i + 1) + "  "
                    + "url:" + urlViewCount.url + " "
                    + "访问量: " + urlViewCount.count + "\n";
                result.append(info);
            }
            result.append("---------------------------\n");
            out.collect(result.toString());
        }
    }


}
