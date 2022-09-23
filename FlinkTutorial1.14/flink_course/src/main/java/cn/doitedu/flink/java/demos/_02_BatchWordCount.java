package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author malichun
 * @create 2022/08/21 0021 20:50
 */
public class _02_BatchWordCount {
    public static void main(String[] args) throws Exception {

        // 批计算入口环境
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();

        // 读数据  -- : 批计算中得到的数据抽象，是一个 DataSet
        DataSource<String> stringDataSource = batchEnv.readTextFile("flink_course/data/wc/input/");

        // 在dataset上调用各种dataset的算子
        stringDataSource
            .flatMap(new MyFlatMapFunction())
            .groupBy(0)
            .sum(1)
            .print();
    }
}

class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String,Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] words = value.split("\\s+");
        for (String word : words) {
            out.collect(Tuple2.of(word,1));
        }
    }
}
