package com.atguigu.chapter11_table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author malichun
 * @create 2022/07/06 0006 23:48
 */
public class Test004_TopNExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 在创建表的DDL中直接定义时间属性
        String createDDL = "CREATE TABLE clickTable (" +
            " `user` STRING," +
            " url STRING, " +
            " ts BIGINT, " +
            " et as TO_TIMESTAMP( FROM_UNIXTIME(ts / 1000))," +
            " WATERMARK FOR et as et - INTERVAL '1' SECOND" +
            ") WITH (" +
            " 'connector' = 'filesystem'," +
            " 'path' = 'input/clicks.txt'," +
            " 'format' = 'csv'" +
            ")";

        tableEnv.executeSql(createDDL);

        // 普通TOP N, 选取当前所有用户中浏览量最大的两个
        Table topResultTable = tableEnv.sqlQuery("select " +
            " user, cnt, row_num " +
            " from (" +
            " select *, row_number() over(order by cnt DESC) as row_num " +
            "  from (" +
            "   select user,count(1) as cnt from clickTable group by user        " +
            ") t" +
            ") where row_num <= 2");

        // 窗口TOP N, 统计一段时间内的(前2名)活跃用户
        String subQuery = " select user, count(url) as cnt, window_start, window_end " +
            " from TABLE(TUMBLE(TABLE clickTable,DESCRIPTOR(et), INTERVAL '10' SECOND))" +
            " GROUP BY user, window_start, window_end ";

        Table windowTopResultTable = tableEnv.sqlQuery("select user, cnt, row_num, window_start, window_end" +
            " from (" +
            "   select * ,row_number() over( partition by window_start, window_end order by cnt desc) as row_num " +
            " from (" +
            subQuery +
            ")" +
            " ) where row_num <= 2");


        //tableEnv.toChangelogStream(topResultTable).print();
        tableEnv.toDataStream(windowTopResultTable).print("window topN : ");

        env.execute();
    }
}
