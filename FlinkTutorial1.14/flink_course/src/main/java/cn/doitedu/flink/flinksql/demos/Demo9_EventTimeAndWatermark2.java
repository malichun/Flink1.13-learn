package cn.doitedu.flink.flinksql.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
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

import static org.apache.flink.table.api.Expressions.$;

/**
 * 流转表 过程中 如何 传承 事件时间 和 watermark
 * {"guid":1, "eventId":"e02","eventTime":1655015710000, "pageId":"p001"}
 */
public class Demo9_EventTimeAndWatermark2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<String> s1 = env.socketTextStream("hadoop102", 8888);

        SingleOutputStreamOperator<Event> s2 = s1.map(s -> JSON.parseObject(s, Event.class))
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.eventTime;
                    }
                }));

        /* 观察流上的watermark 推进 */
        /* s2.process(new ProcessFunction<Event, Object>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, Object>.Context ctx, Collector<Object> out) throws Exception {
                long wm = ctx.timerService().currentWatermark();
                out.collect(value + " => " + wm);
            }
        }).print(); */

        // FIXME 这样: 直接把流 转换成 表 , 会丢失watermark
        tenv.createTemporaryView("t_events", s2);


        /* tenv.executeSql("select guid, eventId, eventTime, pageId, current_watermark(eventTime) from t_events"); */

        // 测试watermark的丢失
        Table table = tenv.from("t_events");
        DataStream<Row> ds = tenv.toDataStream(table);

        ds.process(new ProcessFunction<Row, String>() {
            @Override
            public void processElement(Row value, ProcessFunction<Row, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(value + " => " + ctx.timerService().currentWatermark());
            }
        })/* .print() */;




        // FIXME 可以在流 转换成 表 时,显式声明 watermark策略
        tenv.createTemporaryView("t_events2", s2, Schema.newBuilder()
            .column("guid", DataTypes.INT())
            .column("eventId", DataTypes.STRING())
            .column("eventTime", DataTypes.BIGINT())
            .column("pageId", DataTypes.STRING())

             // FIXME 1. 获取时间属性(a.从流的元数据中获取 b.重新造一个)
            // 利用底层流连接器暴露的 rowtime 元数据(代表的就是底层流中每条数据上的eventTime), 声明成时间时间属性字段
            // 下面定义的列名叫rt, 取自元数据 rowtime; 可以直接用columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)"), 列名就叫rowtime
            .columnByMetadata("rt",DataTypes.TIMESTAMP_LTZ(3) ,"rowtime")
            // .columnByExpression("rt", "to_timestamp_ltz(eventTime,3)") // 转换一个timestamp类型的字段

            // FIXME 2. 生成watermark(a.采用源流的watermark b.重新生成一个 )
            // .watermark("rt", "rt - interval '1' second") // FIXME 重新生成的水位线, 与原流的水位线没有关系了
            .watermark("rt", "SOURCE_WATERMARK()") // 声明watermark,直接引用底层流的watermark,得到与源头watermark完全一致

            .build());


        tenv.executeSql("select guid,eventId, eventTime,pageId, rt, current_watermark(rt) as wm from t_events2")
            .print();

        env.execute();
    }

    public static class Event {
        public int guid;
        public String eventId;
        public long eventTime;
        public String pageId;


        @Override
        public String toString() {
            return "Event{" +
                "guid=" + guid +
                ", eventId='" + eventId + '\'' +
                ", eventTime=" + eventTime +
                ", pageId='" + pageId + '\'' +
                '}';
        }
    }
}
