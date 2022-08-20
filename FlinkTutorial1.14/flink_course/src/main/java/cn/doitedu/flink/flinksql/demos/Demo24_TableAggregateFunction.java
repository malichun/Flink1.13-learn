package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.*;

/**
 *  自定义表聚合函数示例
 *
 *  如果有一种聚合函数, 能够在分组聚合的模式中, 对魅族数据输出多行多列聚合结果
 * @author malichun
 * @create 2022/08/20 0020 11:29
 */
public class Demo24_TableAggregateFunction {

    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        Table table = tenv.fromValues(DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("gender", DataTypes.STRING()),
                DataTypes.FIELD("score", DataTypes.DOUBLE())
            ),
            row(1, "male", "67"),
            row(2, "male", "88"),
            row(3, "male", "98"),
            row(4, "female", "99"),
            row(5, "female", "84"),
            row(6, "female", "89")
        );


        // 用一个聚合函数直接求出每种性别中分数最高的2个分数
        Table t = table
            .groupBy($("gender"))
            //.flatAggregate(call(MyTop2.class, $("score"))) // 依赖 class的FunctionHint
            .flatAggregate(call(MyTop2.class, $("score")).as("score_top", "rank")) // 直接起别名
            .select($("gender"), $("score_top"), $("rank"));

        tenv.executeSql("select * from " + t).print();


    }


    public static class MyAccumulator{
        public Double first;
        public Double second;
    }

    /**
     *  Tuple2<分数, 排名>
     */
    @FunctionHint(output = @DataTypeHint("ROW<score_top DOUBLE, rank INT>"))
    public static class MyTop2 extends TableAggregateFunction<Row, MyAccumulator>{

        @Override
        public MyAccumulator createAccumulator() {
            MyAccumulator myAccumulator = new MyAccumulator();
            myAccumulator.first = Double.MIN_VALUE;
            myAccumulator.second = Double.MIN_VALUE;
            return myAccumulator;
        }

        /**
         * 累加更新逻辑
         * @param acc
         * @param score
         */
        public void accumulate(MyAccumulator acc, Double score) {
            if (score > acc.first) {
                acc.second = acc.first;
                acc.first = score;
            } else if (score > acc.second) {
                acc.second = score;
            }
        }

        public void merge(MyAccumulator acc, Iterable<MyAccumulator> it) {
            for (MyAccumulator otherAcc : it) {
                accumulate(acc, otherAcc.first);
                accumulate(acc, otherAcc.second);
            }
        }

        /**
         * 输出结果, 可以输出多行多列
         * @param acc
         * @param out
         */
        public void emitValue(MyAccumulator acc, Collector<Row> out) {
            // emit the value and rank
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Row.of(acc.first, 1));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Row.of(acc.second, 2));
            }
        }


    }
}
