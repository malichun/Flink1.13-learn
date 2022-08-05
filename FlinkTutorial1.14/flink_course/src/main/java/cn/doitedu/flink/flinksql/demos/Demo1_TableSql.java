package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author malichun
 * @create 2022/08/04 0004 20:46
 */
public class Demo1_TableSql {
    public static void main(String[] args) {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode() // 流计算模式
            .build();

        TableEnvironment tableEnv = TableEnvironment.create(environmentSettings);

        //{"id":1,"name":"zs","age":20,"gender":"male"}
        //{"id":2,"name":"bb","age":30,"gender":"female"}
        //{"id":3,"name":"cc","age":40,"gender":"female"}
        //{"id":14,"name":"dd","age":50,"gender":"male"}

        // 把kafka中的一个topic数据, 映射成一张flinkSql表
        tableEnv.executeSql(
            " create table t_kafka(                                      "
                + "    id int,                                                "
                + "    name string,                                           "
                + "    age int,                                               "
                + "    gender string                                          "
                + ")  WITH (                                                  "
                + " 'connector' = 'kafka',                                    "
                + " 'topic' = 'doit30-2',                                     "
                + " 'properties.bootstrap.servers' = 'hadoop102:9092',        "
                + " 'properties.group.id' = 'g1',                             "
                + " 'format' = 'json',                                        "
                + " 'json.fail-on-missing-field' = 'false',                   "
                + " 'json.ignore-parse-errors' = 'true' ,                     "
                + " 'scan.startup.mode' = 'earliest-offset'                   "
                + ")                                                          "
        );

        /**
         * 把sql表名转换成 Table对象
         */
        Table t_kafka = tableEnv.from("t_kafka");
        // 利用tableApi 进行计算
        t_kafka.groupBy($("gender"))
            .select($("gender"), $("age").avg().as("avg_age"))
            .execute()
            .print();

        TableResult tableResult = tableEnv.executeSql("select gender, avg(age) as avg_age from t_kafka group by gender ");

        tableResult.print();

        // 结果:
        //+----+--------------------------------+-------------+
        //| op |                         gender |     avg_age |
        //+----+--------------------------------+-------------+
        //| +I |                           male |          20 |
        //| +I |                         female |          30 |
        //| -U |                         female |          30 |
        //| +U |                         female |          35 |
        //| -U |                           male |          20 |
        //| +U |                           male |          35 |

    }
}
