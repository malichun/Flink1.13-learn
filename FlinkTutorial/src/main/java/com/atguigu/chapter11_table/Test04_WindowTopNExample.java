package com.atguigu.chapter11_table;

import com.atguigu.chapter05_StreamAPI.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;

/**
 *
 * 下面是一个具体案例的代码实现。由于用户访问事件 Event 中没有商品相关信息，因此我
 * 们统计的是每小时内有最多访问行为的用户，取前两名，相当于是一个每小时活跃用户的查询。
 * @author malichun
 * @create 2022/6/30 0030 18:19
 */
public class Test04_WindowTopNExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源, 并分配时间戳, 生成水位线
        SingleOutputStreamOperator<Event> eventStream = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将数据流转换成表, 并指定时间属性
        Table eventTable = tableEnv.fromDataStream(eventStream, $("user"), $("url"), $("timestamp").rowtime().as("ts"));

        // 为方便在SQL中引用, 在环境中注册表,EventTable
        tableEnv.createTemporaryView("EventTable", eventTable); // 这边区分大小写

        // 定义子查询, 进行窗口聚合, 得到包含窗口信息,用户以及访问次数的结果表
        String subQuery = "select\n" +
                "    window_start,\n" +
                "    window_end,\n" +
                "    user,\n" +
                "    count(url) as cnt\n" +
                "from \n" +
                "    table(tumble( table EventTable, descriptor(ts),interval '1' hour))\n" +
                "group by \n" +
                "    window_start, window_end, user";

        // 定义Top N的外层查询
        String topNQuery = "select\n" +
                "   *\n" +
                "from \n" +
                "(\n" +
                "    select *,\n" +
                "    row_number() over(partition by window_start, window_end order by cnt desc) as row_num\n" +
                "    from (" + subQuery+")    \n" +
                ") t\n" +
                "where row_num <= 2";

        // 执行SQL得到结果表
        Table result = tableEnv.sqlQuery(topNQuery);
        tableEnv.toDataStream(result).print();

        env.execute();
    }
}
