package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * {"guid":1, "eventId":"e02", "eventTime":1655017433000, "pageId":"p001"} // epoch Time
 * {"guid":1, "eventId":"e03", "eventTime":1655017434000, "pageId":"p001"}
 * {"guid":1, "eventId":"e04", "eventTime":1655017435000, "pageId":"p001"}
 * {"guid":1, "eventId":"e05", "eventTime":1655017436000, "pageId":"p001"}
 * {"guid":1, "eventId":"e06", "eventTime":1655017437000, "pageId":"p001"}
 * {"guid":1, "eventId":"e07", "eventTime":1655017438000, "pageId":"p001"}
 * {"guid":1, "eventId":"e08", "eventTime":1655017439000, "pageId":"p001"}
 * @author malc
 * @create 2022/8/10 0010 20:42
 */
public class Demo9_EventTimeAndWatermark {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        /**
         * 只有 TIMESTAMP 或 TIMESTAMP_LTZ 类型的字段可以被生命为rowtime(事件时间属性)
         *
         */
        tenv.executeSql("create table t_events(\n" +
            "    guid int,\n" +
            "    eventId string,\n" +
            /* "    eventTime timestamp(3),\n" + */
            "    eventTime bigint,\n" + // 这个是长整型
            "    pageId string,\n" +
            "    pt as proctime(), " +// 利用一个表达式字段, 来声明processing time属性
            "    rt as TO_TIMESTAMP_LTZ(eventTime, 3),"+  // 将长整型 转换成时间戳
            "    watermark for rt as rt - interval '1' SECOND\n" + // 1毫秒 0.001 利用watermark for xxx 来将一个已定义的TIMESTAMP/TIMESTAMP_LTZ字段声明成时间eventTime属性, 即指定watermark策略
            ")\n" +
            "with (\n" +
            " 'connector' = 'kafka',                                    "
            + " 'topic' = 'doit30-events2',                                     "
            + " 'properties.bootstrap.servers' = 'hadoop02:9092',        "
            + " 'properties.group.id' = 'g1',                             "
            + " 'format' = 'json',                                        "
            + " 'json.fail-on-missing-field' = 'false',                   "
            + " 'json.ignore-parse-errors' = 'true' ,                     "
            + " 'scan.startup.mode' = 'earliest-offset'                   "
            + ")                                                          "
        );

        tenv.executeSql("desc t_events").print();

        tenv.executeSql("select guid,eventId, eventTime,pageId,pt, rt, current_watermark(rt) as wm from t_events").print();
    }
}
