package com.atguigu.chapter11_table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author malichun
 * @create 2022/7/5 0005 9:50
 */
public class Test002_CommonApiTest {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        // 1. 读取数据, 得到DataStream
//        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
//            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
//                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                    @Override
//                    public long extractTimestamp(Event element, long recordTimestamp) {
//                        return element.timestamp;
//                    }
//                })
//            );

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                    .useBlinkPlanner()
                        .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 创建表
        String createDDL = "CREATE TABLE clickTable(" +
            " user_name STRING," +
            " url STRING," +
            " ts BIGINT " +
            ") WITH (" +
            " 'connector' = 'filesystem'," +
            " 'path' = 'input/clicks.txt',"+
            " 'format' = 'csv'" +
            ")";
        tableEnv.executeSql(createDDL);


        // 调用Table Api 进行表的转换
        Table clickTable = tableEnv.from("clickTable");
        Table resultTable = clickTable.where($("user_name").isEqual("Bob"))
                .select($("user_name"), $("url"));

        tableEnv.createTemporaryView("`result`", resultTable);

        // 执行sql进行表查询转换
        Table resultTable2 = tableEnv.sqlQuery("select url, user_name from `result`");

        // 创建一张用于输出的表
        String createOutDDL = "CREATE TABLE outTable(" +
            " url STRING," +
            " user_name STRING" +
            ") WITH (" +
            " 'connector' = 'filesystem',"+
            " 'path' = 'output'," +
            " 'format' = 'csv'"+
            ")";

        tableEnv.executeSql(createOutDDL);

        // 执行聚合计算的查询转换
        Table aggResult = tableEnv.sqlQuery("select user_name,count(url) as cnt from clickTable group by user_name");



        // 创建一张一用于控制台打印的表
        String createPrintOutDDL = "CREATE TABLE outPrintTable(" +
            " url STRING," +
            " cnt BIGINT" +
            ") WITH (" +
            " 'connector' = 'print'"+
            ")";
        tableEnv.executeSql(createPrintOutDDL);

        aggResult.executeInsert("outPrintTable");


//        // 输出表
//        resultTable2.executeInsert("outTable");
//        resultTable2.executeInsert("outPrintTable");
    }
}
