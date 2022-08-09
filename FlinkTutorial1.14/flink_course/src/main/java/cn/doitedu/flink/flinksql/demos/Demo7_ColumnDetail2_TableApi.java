package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 字段定义详解示例
 */
public class Demo7_ColumnDetail2_TableApi {
    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        // 建表(数据源表)
        // {"id":1, "name":"zs","nick":"tiedan","age":18,"gender":"male"}
        // 使用TableApi的方式
        tenv.createTable("t_person",
            TableDescriptor
                .forConnector("kafka")
                .schema(Schema.newBuilder()
                    .column("id", DataTypes.INT()) // column是声明物理字段到表结构中来
                    .column("name", DataTypes.STRING())
                    .column("nick", DataTypes.STRING())
                    .column("age", DataTypes.INT())
                    .column("gender", DataTypes.STRING())
                    .columnByExpression("guid","id")  // 表达式字段
                    /* .columnByExpression("big_age", $("id").plus(10)) */
                    .columnByExpression("big_age","age + 10")
                    // 最后一个isVirtual表示 当这个表被当做sink表示, 该字段是否出现在schema中, 元数据字段
                    .columnByMetadata("offs", DataTypes.BIGINT(),"offset", true)
                    .columnByMetadata("ts",DataTypes.TIMESTAMP_LTZ(3), "timestamp",true)
                    //.primaryKey("id","name") // 定义主键
                    .build())
                .format("json")
                .option("topic","doit30-4")
                .option("properties.bootstrap.servers","hadoop102:9092")
                .option("properties.group.id","g1")
                .option("json.fail-on-missing-field","false")
                .option("json.ignore-parse-errors","true")
                .option("scan.startup.mode","earliest-offset")
            .build());

        //
        //tenv.executeSql(
        //    "create table t_person (                              " +
        //        "    id int ,                                              " + //--primary key not enforced
        //        "    name string,                                          " +
        //        "    nick string,                                          " +
        //        "    age int,                                              " + // -- 物理字段
        //        "    gender string,                                        " +
        //        "    guid as id,                                            "+  // 表达式字段
        //        "    big_age as age + 10,                                  " +  // 表达式字段(逻辑字段)
        //        "    offs bigint metadata from 'offset'   ,                " + // 元数据字段
        //        "    ts TIMESTAMP_LTZ(3) metadata from 'timestamp'        " + // 元数据字段
        //        //"    PRIMARY KEY(id,name) NOT ENFORCED                    " + // 主键约束
        //        ")  WITH (                                                 " +
        //        " 'connector' = 'kafka',                                   " +
        //        " 'topic' = 'doit30-4',                                    " +
        //        " 'properties.bootstrap.servers' = 'hadoop102:9092',       " +
        //        " 'properties.group.id' = 'g1',                            " +
        //        " 'format' = 'json',                                       " +
        //        " 'json.fail-on-missing-field' = 'false',                  " +
        //        " 'json.ignore-parse-errors' = 'true' ,                    " +
        //        " 'scan.startup.mode' = 'earliest-offset'                  " +
        //        //" 'scan.startup.mode' = 'latest-offset'                     " +
        //        ")  ");

        tenv.executeSql("desc t_person").print();

        tenv.executeSql("select * from t_person").print();
    }
}
