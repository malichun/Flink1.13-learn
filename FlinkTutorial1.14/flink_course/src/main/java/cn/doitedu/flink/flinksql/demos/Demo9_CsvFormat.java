package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * csv format详解
 * @author malichun
 * @create 2022/08/10 0010 1:00
 */
public class Demo9_CsvFormat {
    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());


        // 数据格式:
        //"1","zs","18"
        //"2","ls","20"
        //,"ww","25"

        tenv.executeSql("create table t_csv (\n" +
            "    id int,\n" +
            "    name string,\n" +
            "    age string\n" +
            ") with (                                                             \n" +
            "'connector' = 'filesystem',                                          \n" +
            "'path' = 'FlinkTutorial1.14\\flink_course\\data\\csv\\a.csv',   \n" +
            "'format' = 'csv',\n" +
            "'csv.field-delimiter' = ',',\n" +
            "'csv.disable-quote-character'  = 'false',\n" +
            "'csv.quote-character' = '|',\n" +
            "'csv.ignore-parse-errors' = 'true' ,\n" +
            "'csv.null-literal' = '\\N' ," + // 把csv中\N 表示为null
            " 'csv.allow-comments' = 'true'"+
            ")     \n");

        tenv.executeSql("select * from t_csv").print();

    }
}
