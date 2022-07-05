package com.atguigu.chapter11_table;

import com.atguigu.chapter05_StreamAPI.ClickSource;
import com.atguigu.chapter05_StreamAPI.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author malichun
 * @create 2022/7/4 0004 18:03
 */
public class Test001_SimpleTableExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 读取数据, 得到DataStream
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })
            );

        // 2.创建一个表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3. 将DataStream转换成表
        Table eventTable = tableEnv.fromDataStream(eventStream);


        // 打印schema
//        eventTable.printSchema();
//        (
//  `user` STRING,
//  `url` STRING,
//  `timestamp` BIGINT
//)

        // 4. 直接写sql进行转换
        Table resultTable1 = tableEnv.sqlQuery("select user, url, `timestamp` as ts from " + eventTable);

        // 5. 基于Table直接进行转转
        Table resultTable2 = eventTable.select($("user"), $("url"), $("timestamp").as("ts"))
            .where($("user").isEqual("Alice"));

        // 6. 将表转换成流打印输出
        tableEnv.toDataStream(resultTable1).print("result1");
        tableEnv.toDataStream(resultTable2).print("result2");

        // 7. 聚合转换
        tableEnv.createTemporaryView("clickTable", eventTable);
        Table aggResult = tableEnv.sqlQuery("select user, count(1) as cnt from clickTable group by user");

        tableEnv.toChangelogStream(aggResult).print();

        env.execute();
    }
}
