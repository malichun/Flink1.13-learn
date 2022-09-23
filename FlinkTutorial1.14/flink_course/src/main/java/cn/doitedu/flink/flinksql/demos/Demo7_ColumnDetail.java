package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 字段定义详解示例
 */
public class Demo7_ColumnDetail {
    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        // 建表(数据源表)
        // {"id":1, "name":"zs","nick":"tiedan","age":18,"gender":"male"}
        tenv.executeSql(
            "create table t_person (                              " +
                "    id int ,                                              " + //--primary key not enforced
                "    name string,                                          " +
                "    nick string,                                          " +
                "    age int,                                              " + // -- 物理字段
                "    gender string,                                        " +
                "    guid as id,                                            "+  // 表达式字段
                "    big_age as age + 10,                                  " +  // 表达式字段(逻辑字段)
                "    offs bigint metadata from 'offset'   ,                " + // 元数据字段
                "    ts TIMESTAMP_LTZ(3) metadata from 'timestamp'        " + // 元数据字段
                //"    PRIMARY KEY(id,name) NOT ENFORCED                    " + // 主键约束
                ")  WITH (                                                 " +
                " 'connector' = 'kafka',                                   " +
                " 'topic' = 'doit30-4',                                    " +
                " 'properties.bootstrap.servers' = 'hadoop102:9092',       " +
                " 'properties.group.id' = 'g1',                            " +
                " 'format' = 'json',                                       " +
                " 'json.fail-on-missing-field' = 'false',                  " +
                " 'json.ignore-parse-errors' = 'true' ,                    " +
                " 'scan.startup.mode' = 'earliest-offset'                  " +
                //" 'scan.startup.mode' = 'latest-offset'                     " +
                ")  ");

        tenv.executeSql("desc t_person").print();

        tenv.executeSql("select * from t_person").print();
    }
}
