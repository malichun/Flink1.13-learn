package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * {"guid":1, "eventId":"e02","eventTime":1655015711000, "pageId":"p001"}
 * {"guid":1, "eventId":"e02","eventTime":1655015712000, "pageId":"p001"}
 * {"guid":1, "eventId":"e02","eventTime":1655015713000, "pageId":"p001"}
 * {"guid":1, "eventId":"e02","eventTime":1655015714000, "pageId":"p001"}
 * {"guid":1, "eventId":"e02","eventTime":1655015715000, "pageId":"p001"}
 * {"guid":1, "eventId":"e02","eventTime":1655015716000, "pageId":"p001"}
 * {"guid":1, "eventId":"e02","eventTime":1655015717000, "pageId":"p001"}
 * {"guid":1, "eventId":"e02","eventTime":1655015718000, "pageId":"p001"}
 *
 */
public class Demo9_EventTimeAndWatermark {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        tenv.executeSql("create table t_events(\n" +
            "    guid int, \n" +
            "    eventId string, \n" +
            "    eventTime timestamp(3),\n" +
            "    pageId string,\n" +
            "    watermark for eventTime as eventTime - interval  '1' second\n" +
            ") with (\n" +
            "    'connector' = 'kafka',                                   \n" +
            "    'topic' = 'doit30-event',                             \n" +
            "    'properties.bootstrap.servers' = 'hadoop102:9092',    \n" +
            "    'properties.group.id' = 'g1',                         \n" +
            "    'format' = 'json',                                    \n" +
            "    'json.fail-on-missing-field' = 'false',               \n" +
            "    'json.ignore-parse-errors' = 'true' ,                 \n" +
            "    'scan.startup.mode' = 'earliest-offset'               \n" +
            ")");

        tenv.executeSql("desc t_events").print();

        tenv.executeSql("select guid, eventId, eventTime, pageId, CURRENT_WATERMARK(eventTime) as wm  from t_events").print();

    }
}
