package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * 作为source
 * scan 模式, 只会读取一次, 不会持续读
 */
public class Demo12_JdbcConnectorTest1 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 建表来映射 mysql中的 flinktest.stu
        tenv.executeSql(" create table flink_stu(                                 "
            + "     id int primary key not enforced,                    "
            + "     name string,                                        "
            + "     age int,                                            "
            + "     gender string                                       "
            + " ) with (                                                "
            + "     'connector' = 'jdbc',                               "
            + "     'url' = 'jdbc:mysql://hadoop102:3306/flinktest',    "
            + "     'table-name' = 'stu',                               "
            + "     'username' = 'root',                                "
            + "     'password' = '123456'                               "
            + " )                                                       ");

        tenv.executeSql("select * from flink_stu").print();


    }
}
