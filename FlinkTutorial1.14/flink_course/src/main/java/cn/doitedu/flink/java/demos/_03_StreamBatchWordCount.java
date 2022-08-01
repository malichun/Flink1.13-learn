package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 流批一体
 * @author malc
 * @create 2022/8/1 0001 19:21
 */
public class _03_StreamBatchWordCount {
    public static void main(String[] args) throws Exception {
        // 流处理编程环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 读文件, 得到dataStream
        DataStreamSource<String> streamSource = env.readTextFile("FlinkTutorial1.14/flink_course/data/wc/input/wc.txt");

        // 调用dataStream算计做计算
        streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                    Arrays.stream(s.split(" ")).forEach(e -> out.collect(Tuple2.of(e, 1)));
                }
            })
            .keyBy(t -> t.f0)
            .sum(1)
            .print();

        env.execute();
    }
}
