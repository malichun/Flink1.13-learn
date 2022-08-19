package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;


/**
 * 自定义标量函数
 * @author malichun
 * @create 2022/08/19 0019 0:19
 */
public class Demo21_CustomScalarFunction {
    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());


        Table table = tenv.fromValues(
            DataTypes.ROW(DataTypes.FIELD("name", DataTypes.STRING())),
            Row.of("aaa"),
            Row.of("bbb"),
            Row.of("ccc")
        );

        tenv.createTemporaryView("t", table);

        // 注册自定你函数
        tenv.createTemporarySystemFunction("my_upper",MyUpper.class);

        tenv.executeSql("select my_upper(name) from t").print();


    }

    public static class MyUpper extends ScalarFunction{

        public String eval(String str){
            return str.toUpperCase();
        }
    }
}
