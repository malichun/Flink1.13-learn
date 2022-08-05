package cn.doitedu.flink.java.demos;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.Arrays;
import java.util.List;

/**
 *
 * 源测试
 * @author malichun
 * @create 2022/08/01 0001 22:40
 */
public class _05_SourceOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> fromElements = env.fromElements(1, 2, 3, 4, 5);
        fromElements.print();

        // 并行的
        DataStreamSource<Long> longDataStreamSource = env.generateSequence(1, 100L);

        // 并行的
        List<String> dataList = Arrays.asList("a", "b", "c", "d");
        DataStreamSource<Long> longDataStreamSource1 = env.fromParallelCollection(
            new NumberSequenceIterator(1L, 10L),
            Long.class
        );


        env.execute();
    }
}
