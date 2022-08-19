package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.*;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.row;

/**
 * 表函数
 *
 * @author malichun
 * @create 2022/08/20 0020 0:18
 */
public class Demo23_TableFunction {

    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
/*
        Table table = tenv.fromValues(DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("phone_numbers", DataTypes.ARRAY(DataTypes.STRING()))),
            Row.of(1, "zs", Expressions.array("138","139","135")),
            row(1, "zs", Expressions.array("135","136"))
        );

        tenv.createTemporaryView("t", table);
    //SELECT order_id, tag
    //FROM Orders CROSS JOIN UNNEST(tags) AS t (tag)

        tenv.executeSql("select id,name,pn from t cross join unnest(phone_numbers) as t1 (pn)").print();
*/

        Table table = tenv.fromValues(DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("phone_numbers", DataTypes.STRING())),
            Row.of(1, "zs", "138,139,135"),
            row(2, "zs", "135,136")
        );

        tenv.createTemporaryView("t", table);

        tenv.createTemporarySystemFunction("mysplit", MySplitFunction.class);

        //SELECT myField, word, length " +
        //  "FROM MyTable, LATERAL TABLE(SplitFunction(myField)) as t(word,length)

        // 展开手机号字符串
        tenv.executeSql("select id,name,pn,length from t, lateral table(mysplit(phone_numbers,',')) as t1(pn, length)")/* .print() */;


        //// call registered function in SQL
        //env.sqlQuery(
        //  "SELECT myField, word, length " +
        //  "FROM MyTable, LATERAL TABLE(SplitFunction(myField))");
        //env.sqlQuery(
        //  "SELECT myField, word, length " +
        //  "FROM MyTable " +
        //  "LEFT JOIN LATERAL TABLE(SplitFunction(myField)) ON TRUE");
        //
        //// rename fields of the function in SQL
        //env.sqlQuery(
        //  "SELECT myField, newWord, newLength " +
        //  "FROM MyTable " +
        //  "LEFT JOIN LATERAL TABLE(SplitFunction(myField)) AS T(newWord, newLength) ON TRUE");

        tenv.executeSql("select id,name,pnn,length from t left join lateral table(mysplit(phone_numbers,',')) as t1(pnn,length) on true").print();




    }

    @FunctionHint(output = @DataTypeHint("ROW<pn STRING, length INT>"))
    public static class MySplitFunction extends TableFunction<Row>{

        public void eval(String str, String delimiter){
            for (String s : str.split(delimiter)) {
                collect(Row.of(s, s.length()));
            }
        }
    }

}
