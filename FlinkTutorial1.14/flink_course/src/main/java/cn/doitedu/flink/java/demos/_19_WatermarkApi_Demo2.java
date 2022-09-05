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
 * 测试多并行度算子
 * @author malichun
 * @create 2022/09/02 0002 22:00
 */
public class _19_WatermarkApi_Demo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.e01,168673487846,pg01
        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9999);


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
        ).setParallelism(2);

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
        }).setParallelism(1).print();


        env.execute();
    }
}
