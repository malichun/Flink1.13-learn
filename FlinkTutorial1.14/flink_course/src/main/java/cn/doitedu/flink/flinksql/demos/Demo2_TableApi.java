package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author malichun
 * @create 2022/08/04 0004 21:58
 */
public class Demo2_TableApi {
    public static void main(String[] args) {
        // 1. 纯粹表环境创建
        //EnvironmentSettings settings = EnvironmentSettings.newInstance()
        //    .inStreamingMode()
        //    .build();
        //TableEnvironment tenv = TableEnvironment.create(settings);

        // 2. 混合环境创建
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 建表
        Table table = tenv.from(
            TableDescriptor.forConnector("kafka") // 指定连接器
            .schema(Schema.newBuilder()  // 指定表结构
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .column("gender", DataTypes.STRING())
                .build()
            )
            .format("json") // 指定数据源的数据格式
            .option("topic","doit30-2")
            .option("properties.bootstrap.servers", "hadoop102:9092")
            .option("properties.group.id", "g1")
            .option("scan.startup.mode", "earliest-offset")
            .option("json.fail-on-missing-field", "false")
            .option("json.ignore-parse-errors", "true")
            .build());


        // 查询
        table.execute().print();
        Table table2 = table.groupBy($("gender"))
            .select($("age").avg().as("avg_age"), $("gender"));


        /**
         *
         * 将 一个已创建好的table对象， 注册成sql中的视图
         */
        tenv.createTemporaryView("kafka_table", table);

        tenv.executeSql("select gender, avg(age) as avg_age from kafka_table group by gender").print();

        // 输出
        //table2.execute().print();
    }
}
