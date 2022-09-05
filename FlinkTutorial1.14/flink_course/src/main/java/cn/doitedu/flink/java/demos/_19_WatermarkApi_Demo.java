package cn.doitedu.flink.java.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author malichun
 * @create 2022/09/02 0002 22:00
 */
public class _19_WatermarkApi_Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.e01,168673487846,pg01
        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9999);
        env.getConfig().setAutoWatermarkInterval(1000);

        // 策略1: WatermarkStrategy.noWatermarks() 不生成 watermark, 禁用了事件时间的推进机制
        // 策略3: WatermarkStrategy.forMonotonousTimestamps 紧跟最大事件时间
        // 策略2: WatermarkStrategy.forBoundedOutOfOrderness() 允许乱序watermark生成策略
        // 策略4: WatermarkStrategy.forGenerator() 自定义watermark生成

        /**
         * 示例1: 从最源头算子开始生成watermark
         */

        // 一. 构造一个watermark的生成策略, (算法策略, 以及 事件时间的抽取方法)
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
            .<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
            .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                @Override
                public long extractTimestamp(String element, long recordTimestamp) {
                    return Long.parseLong(element.split(",")[2]);
                }
            });
        // 二.然后, 将构造好的watermark策略对象, 分配给流(source算子)
       // s1.assignTimestampsAndWatermarks(watermarkStrategy);


        /**
         * 示例2: 不是从最源头算子开始生成watermark, 而是从最源头算子开始生成watermark
         */
        SingleOutputStreamOperator<EventBean> s2 = s1.map(s -> {
            String[] arr = s.split(",");
            return new EventBean(Long.parseLong(arr[0]), arr[1], Long.parseLong(arr[2]), arr[3]);
        }).assignTimestampsAndWatermarks(
            //WatermarkStrategy.<EventBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
            WatermarkStrategy.<EventBean>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                    @Override
                    public long extractTimestamp(EventBean element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                })
        );

        s2.process(new ProcessFunction<EventBean, EventBean>() {
            @Override
            public void processElement(EventBean eventBean, ProcessFunction<EventBean, EventBean>.Context ctx, Collector<EventBean> out) throws Exception {
                // 打印此刻的watermark
                long processingTime = ctx.timerService().currentProcessingTime(); // 当前的处理时间
                long watermark = ctx.timerService().currentWatermark();
                System.out.println("本次收到数据: "+ eventBean);
                System.out.println("此刻的watermark: " + watermark);
                System.out.println("此刻的处理时间: " + processingTime);

                out.collect(eventBean);
            }
        }).print();


        env.execute();
    }
}

@Data
@NoArgsConstructor
@AllArgsConstructor
class EventBean{
    private long guid;
    private String eventId;
    private long timestamp;
    private String pageId;
}
