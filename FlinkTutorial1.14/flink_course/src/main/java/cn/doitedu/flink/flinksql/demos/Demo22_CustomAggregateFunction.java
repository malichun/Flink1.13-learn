package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;


/**
 * 自定义聚合函数, 求平均
 * @author malichun
 * @create 2022/08/19 0019 0:19
 */
public class Demo22_CustomAggregateFunction {
    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());


        Table table = tenv.fromValues(
            DataTypes.ROW(
                DataTypes.FIELD("uid", DataTypes.INT()),
                DataTypes.FIELD("gender", DataTypes.STRING()),
                DataTypes.FIELD("score", DataTypes.DOUBLE())
            ),
            Row.of(1, "male", 80),
            Row.of(2, "male", 100),
            Row.of(3,"female", 90)
        );

        tenv.createTemporaryView("t", table);

        // 注册自定义函数
        tenv.createTemporarySystemFunction("myavg",MyAvg.class);

        tenv.executeSql("select gender,myavg(score) as avg_score  from t group by gender").print();


    }


    public static class MyAccumulator{
        public int count;
        public double sum;
    }

    public static class MyAvg extends AggregateFunction<Double, MyAccumulator>{
        /**
         * 获取累加器的值
         * @param accumulator
         * @return
         */
        @Override
        public Double getValue(MyAccumulator accumulator) {
            return accumulator.sum / accumulator.count;
        }
        /**
         * 创建累加器
         * @return
         */
        @Override
        public MyAccumulator createAccumulator() {
            MyAccumulator myAccumulator = new MyAccumulator();
            myAccumulator.sum = 0;
            myAccumulator.count = 0;
            return myAccumulator;
        }

        public void accumulate(MyAccumulator acc,Double score) {
            acc.sum += score;
            acc.count += 1;
        }
    }
}
