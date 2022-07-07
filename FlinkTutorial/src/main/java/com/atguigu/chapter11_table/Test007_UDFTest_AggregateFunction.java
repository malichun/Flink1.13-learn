package com.atguigu.chapter11_table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;

/**
 * @author malichun
 * @create 2022/07/07 0007 23:26
 */
public class Test007_UDFTest_AggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 在创建表的DDL中直接定义时间属性
        String createDDL = "CREATE TABLE clickTable(" +
            " `user` STRING," +
            " url STRING, " +
            " ts BIGINT," +
            " et as to_timestamp(from_unixtime(ts/1000))," +
            " watermark for et as et - interval '1' second" +
            ") with (" +
            " 'connector'='filesystem'," +
            " 'path' = 'input/clicks.txt'," +
            " 'format' = 'csv')";
        tableEnv.executeSql(createDDL);

        // 2. 注册自定义聚合函数
        tableEnv.createTemporarySystemFunction("WeightedAverage", WeightedAverage.class);

        // 3. 调用UDAF进行查询转换
        Table resultTable = tableEnv.sqlQuery("select user, WeightedAverage(ts,1) as w_avg " +
            " from clickTable group by user");

        // 4. 转换成流打印输出
        tableEnv.toChangelogStream(resultTable).print();

        env.execute();
    }

    // 单独定义一个累加器类型
    public static class WeightedAvgAccumulator{
        public long sum = 0;
        public int count = 0;
    }

    // 实现一个自定义聚合函数, 计算加权平均值
    public static class WeightedAverage extends AggregateFunction<Long, WeightedAvgAccumulator>{

        @Override
        public Long getValue(WeightedAvgAccumulator accumulator) {
            if(accumulator.count == 0){
                return null;
            }
            return accumulator.sum / accumulator.count;
        }

        // 初始化累加器
        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        // 累加计算的方法
        public void accumulate(WeightedAvgAccumulator accumulator, Long iValue, Integer iWeight){
            accumulator.sum += iValue * iWeight;
            accumulator.count += iWeight;
        }
    }


}
