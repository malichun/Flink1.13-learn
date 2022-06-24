package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author malichun
 * @create 2022/6/19 13:59
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 从文件读取数据 按行读取(存储的元素就是每行的文本)
        DataSource<String> lineDataSource = env.readTextFile("input/words.txt");

        // 3. 将每行数据进行分词, 然后转换成二元组类型
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDataSource
                .flatMap((String s, Collector<Tuple2<String, Long>> collector) -> {
                    String[] arr = s.split(" ");
                    Arrays.stream(arr).forEach(word -> collector.collect(Tuple2.of(word, 1L)));
                }).returns(Types.TUPLE(Types.STRING, Types.LONG)); ////当 Lambda 表达式使用 Java 泛型的时候, 由于泛型擦除的存在, 需要显示的声明类型信息


        // 4. 按照word进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOne
                .groupBy(0);

        // 5. 分组内进行聚合统计
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup
                .sum(1);

        // 6. 打印结果
        sum.print();


    }
}
