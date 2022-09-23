package cn.doitedu.flink.flinksql.demos;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 表转流
 * {"guid":1, "eventId":"e02","eventTime":1655015710000, "pageId":"p001"}
 */
public class Demo9_EventTimeAndWatermark3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        tenv.executeSql("create table t_events(\n" +
            "    guid int,\n" +
            "    eventId string,\n" +
            /* "    eventTime timestamp(3),\n" + */ // 这个要用 2022-08-11 12:12:12.000 的字符串来表示时间戳,
            "    eventTime bigint,\n" + // 这个是长整型的 epoch 时间
            "    pageId string,\n" +
            "    pt as proctime(), " +// 利用一个表达式字段, 来声明processing time属性
            "    rt as TO_TIMESTAMP_LTZ(eventTime, 3),"+  // 将长整型 转换成时间戳
            "    watermark for rt as rt - interval '1' SECOND\n" + // 1毫秒 0.001 利用watermark for xxx 来将一个已定义的TIMESTAMP/TIMESTAMP_LTZ字段声明成时间eventTime属性, 即指定watermark策略
            ")\n" +
            "with (\n" +
            " 'connector' = 'kafka',                                    "
            + " 'topic' = 'doit30-events2',                                     "
            + " 'properties.bootstrap.servers' = 'hadoop102:9092',        "
            + " 'properties.group.id' = 'g1',                             "
            + " 'format' = 'json',                                        "
            + " 'json.fail-on-missing-field' = 'false',                   "
            + " 'json.ignore-parse-errors' = 'true' ,                     "
            + " 'scan.startup.mode' = 'earliest-offset'                   "
            + ")                                                          "
        );

        DataStream<Row> ds = tenv.toDataStream(tenv.from("t_events"));

        ds.process(new ProcessFunction<Row, Object>() {
            @Override
            public void processElement(Row value, ProcessFunction<Row, Object>.Context ctx, Collector<Object> out) throws Exception {
                out.collect(value + " => " + ctx.timerService().currentWatermark());
            }
        }).print();

        env.execute();

    }
}
