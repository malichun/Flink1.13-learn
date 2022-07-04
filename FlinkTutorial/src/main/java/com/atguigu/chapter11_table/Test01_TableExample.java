package com.atguigu.chapter11_table;

import com.atguigu.chapter05_StreamAPI.Event;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author malichun
 * @create 2022/6/29 16:35
 */
public class Test01_TableExample {
    public static void main(String[] args) throws Exception{
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源
        DataStreamSource<Event> eventStream = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L),
                new Event("Bob", "./prod?id=3", 90 * 1000L),
                new Event("Alice", "./prod?id=7", 105 * 1000L)
        );

        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将数据流转换成表
        Table eventTable = tableEnv.fromDataStream(eventStream);

        // 用执行SQL的方式提取数据
        Table visitTable = tableEnv.sqlQuery("select url, user from " + eventTable);
        Table clickTable2 = eventTable.select($("url"), $("user"));

        Table table3 = tableEnv.sqlQuery("select user,url from " + eventTable + " where user = 'Alice'");

        Table urlCountTable = tableEnv.sqlQuery("select user,count(url) as cnt from " + eventTable + " group by user");

        tableEnv.createTemporaryView("EventTable", eventTable);


//        tableEnv.toDataStream(table3).print();


        Table aliceVisitTable = tableEnv.sqlQuery("select user, url from EventTable where user = 'Alice'");
//        tableEnv.toDataStream(aliceVisitTable).print();


//        tableEnv.toChangelogStream(urlCountTable).print();

        DataStream<Row> dataStream = env.fromElements(
                Row.ofKind(RowKind.INSERT, "Alice", 12),
                Row.ofKind(RowKind.INSERT, "Bob", 5),
                Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
                Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100)
        );

        // 将更新日志流转成表
        Table table = tableEnv.fromChangelogStream(dataStream);


        // 将表转换成数据流, 打印输出
//        tableEnv.toDataStream(clickTable2).print();

        env.execute();
    }
}
