package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.array;
import static org.apache.flink.table.api.Expressions.row;

/**
 * @author malichun
 * @create 2022/08/17 0017 23:45
 */
public class Demo19_ArrayJoin {
    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        Table table = tenv.fromValues(DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("name", DataTypes.STRING()),
            DataTypes.FIELD("tags", DataTypes.ARRAY(
                DataTypes.STRING()
            ))
        ),
            row("1","zs",array("stu", "child"))
            ,row("2","bb", array("miss"))
        );

        tenv.createTemporaryView("t", table);

        tenv.executeSql("select t.id, t.name, x.tag from t cross join unnest(tags) as x(tag)")/* .print() */;
        /*
        *   +----+-------------+--------------------------------+--------------------------------+
            | op |          id |                           name |                            tag |
            +----+-------------+--------------------------------+--------------------------------+
            | +I |           1 |                             zs |                            stu |
            | +I |           1 |                             zs |                          child |
            | +I |           2 |                             bb |                           miss |
            +----+-------------+--------------------------------+--------------------------------+
        * */

        tenv.createTemporaryFunction("mysplit", MySplit.class);

        tenv.executeSql("select t.id, t.name, tag from t, lateral table(mysplit(tags))")/* .print() */;
        /*
        +----+-------------+--------------------------------+--------------------------------+
        | op |          id |                           name |                            tag |
        +----+-------------+--------------------------------+--------------------------------+
        | +I |           1 |                             zs |                            stu |
        | +I |           1 |                             zs |                          child |
        | +I |           2 |                             bb |                           miss |
        +----+-------------+--------------------------------+--------------------------------+
        * */

        tenv.executeSql("select t.id, t.name, x.tag2 from t,lateral table(mysplit(tags)) x(tag2)")/* .print() */;
        /*
        +----+-------------+--------------------------------+--------------------------------+
        | op |          id |                           name |                           tag2 |
        +----+-------------+--------------------------------+--------------------------------+
        | +I |           1 |                             zs |                            stu |
        | +I |           1 |                             zs |                          child |
        | +I |           2 |                             bb |                           miss |
        +----+-------------+--------------------------------+--------------------------------+
         */
        tenv.executeSql("select t.id,t.name,tag from t left join lateral table(mysplit(tags)) on true")/* .print() */;
        /*
        +----+-------------+--------------------------------+--------------------------------+
        | op |          id |                           name |                            tag |
        +----+-------------+--------------------------------+--------------------------------+
        | +I |           1 |                             zs |                            stu |
        | +I |           1 |                             zs |                          child |
        | +I |           2 |                             bb |                           miss |
        +----+-------------+--------------------------------+--------------------------------+
         */

        tenv.executeSql("select t.id,t.name,x.tag2 from t left join lateral table(mysplit(tags)) x(tag2) on true").print();
        /*
        +----+-------------+--------------------------------+--------------------------------+
        | op |          id |                           name |                           tag2 |
        +----+-------------+--------------------------------+--------------------------------+
        | +I |           1 |                             zs |                            stu |
        | +I |           1 |                             zs |                          child |
        | +I |           2 |                             bb |                           miss |
        +----+-------------+--------------------------------+--------------------------------+
         */
    }

    @FunctionHint(output = @DataTypeHint("Row<tag string>"))
    public static class MySplit extends TableFunction<Row>{
        public void eval(String[] arr){
            for(String s: arr){
                collect(Row.of(s));
            }
        }
    }
}
