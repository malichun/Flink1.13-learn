package com.atguigu.chapter11_table;

import com.atguigu.chapter05_StreamAPI.ClickSource;
import com.atguigu.chapter05_StreamAPI.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author malichun
 * @create 2022/7/5 0005 17:43
 */
public class Test003_TimeAndWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 在创建表的ddl中直接定义时间属性
        String createDDL = "CREATE TABLE clickTable(" +
            " user_name STRING," +
            " url STRING," +
            " ts BIGINT, " +
            " et as TO_TIMESTAMP( from_unixtime(ts/1000))," + // 新的时间属性
            " watermark for et as et - interval '5' second" +
            ") WITH(" +
            "'connector' = 'filesystem'," +
            "'path'='input/clicks.txt'," +
            "'format'='csv'" +
            ")";

        tableEnv.executeSql(createDDL);

        // 2. 在流转换成Table的时候定义时间属性
        SingleOutputStreamOperator<Event> clickStream = env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));
        Table clickTable = tableEnv.fromDataStream(
            clickStream,
            $("user"), $("url"), $("timestamp").as("ts"), $("et").rowtime() //watermark
        );
        clickTable.printSchema();

        // 聚合查询转换
        // 1.分组聚合
        Table aggTable = tableEnv.sqlQuery("select user_name,count(1) from clickTable group by user_name");


        // 2. 分组窗口函数, 过期了, 老版本
        Table tumbleGroupResult = tableEnv.sqlQuery("select " +
            " user_name, count(1) as cnt, " +
            " TUMBLE_END(et, INTERVAL '10' SECOND) AS endT " +
            " from clickTable" +
            " group by user_name,TUMBLE(et, INTERVAL '10' SECOND)");

        // 3. 窗口函数
        // 3.1 滚动窗口
        Table tumbleWindowResult = tableEnv.sqlQuery("select user_name, count(1) as cnt, " +
            " window_end as endT" +
            " from TABLE(TUMBLE(TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND)) " +
            "group by user_name,window_start, window_end");


        // 3.2 滑动窗口
        Table hopWindowResult = tableEnv.sqlQuery("select user_name, count(1) as cnt," +
            " window_end as endT" +
            " from TABLE(HOP(TABLE clickTable, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND)) " +
            " GROUP BY user_name, window_end, window_start");

        // 3.3 累计窗口
        Table cumulateWindowResultTable = tableEnv.sqlQuery("select user_name, count(1) as cnt," +
            " window_end as endT" +
            " from TABLE(CUMULATE(TABLE clickTable, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND))" +
            " GROUP BY user_name, window_end, window_start");

        // 4. 开窗聚合(over)
        // 统计每个用户之前3次访问的平均时间戳
        Table overWindowResult = tableEnv.sqlQuery("select user_name, " +
            " avg(ts) over(PARTITION BY user_name order by et rows between 3 preceding and current row) as avg_ts " +
            " from clickTable");




        //tableEnv.toChangelogStream(aggTable).print("agg");
        //tableEnv.toChangelogStream(tumbleGroupResult).print("group window");
        //tableEnv.toChangelogStream(tumbleWindowResult).print("tumble window");
        //tableEnv.toChangelogStream(hopWindowResult).print("hop window");
        //tableEnv.toChangelogStream(cumulateWindowResultTable).print("cumulate window");
        tableEnv.toDataStream(overWindowResult).print("over window: ");

        env.execute();
    }
}
