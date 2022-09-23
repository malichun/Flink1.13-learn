package cn.doitedu.flink.java.exercise;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.w3c.dom.TypeInfo;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;
import java.util.prefs.BackingStoreException;

/**
 * 练习1
 * 创建两个流
 * 流1:
 * id,eventId,cnt
 * 1,event01,3
 * 1,event02,2
 * <p>
 * 流2:
 * id, gender, city
 * 1, male, shanghai
 * 2, female, beijing
 * <p>
 * 需求:
 * 1.将流1的数据展开
 * 一条数据 1,event01,3
 * 需要展开成3条
 * 1,event01,随机数1
 * 1,event01,随机数2
 * 1,event01,随机数3
 * 2.流1的数据,还需要关联上 流2 的数据 (性别,城市)
 * 并且把关联失败的流1的数据, 写入一个测流, 否则写入主流
 * <p>
 * 3.把关联上的数据做一个测流输出(随机数能被7整除的数据,放入测流中,其他数据保留在主流中)
 * <p>
 * 4.然后对关联好的数据,按性别分组, 取最大随机数所在的那条数据作为结果输出
 * <p>
 * 5.把测流处理结果写入文件系统, 并写成parquet格式
 * <p>
 * 6.把主流处理结果写入mysql,并实现幂等更新
 *
 * @author malichun
 * @create 2022/08/31 0031 21:33
 */
public class Exercise_1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        DataStreamSource<String> ds1 = env.socketTextStream("localhost", 9998);

        SingleOutputStreamOperator<EventCount> s1 = ds1.map(new MapFunction<String, EventCount>() {
            @Override
            public EventCount map(String value) throws Exception {
                String[] arr = value.split(",");
                return new EventCount(Integer.parseInt(arr[0]), arr[1], Integer.parseInt(arr[2]));
            }
        });

        // 创建流2
        DataStreamSource<String> ds2 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<UserInfo> s2 = ds2.map(s -> {
            String[] arr = s.split(",");
            return new UserInfo(Integer.parseInt(arr[0]), arr[1], arr[2]);
        });

        // 1.对流1的数据按count值 展开
        SingleOutputStreamOperator<EventCount> flattend = s1.process(new ProcessFunction<EventCount, EventCount>() {
            @Override
            public void processElement(EventCount value, ProcessFunction<EventCount, EventCount>.Context ctx, Collector<EventCount> out) throws Exception {
                int cnt = value.getCnt();
                for (int i = 0; i < cnt; i++) {
                    out.collect(new EventCount(value.getId(), value.getEventId(), RandomUtils.nextInt(10, 100)));
                }
            }
        });

        // 2.流1 的数据关联上流2的数据(性别, 城市)
        // 通过场景分析, 用广播状态最合适
        MapStateDescriptor<Integer, UserInfo> stateDescriptor = new MapStateDescriptor<Integer, UserInfo>("userInfoDescriptor", Integer.class, UserInfo.class);
        // 准备侧流输出标签
        OutputTag<EventCount> cOutputTag = new OutputTag<>("c", TypeInformation.of(EventCount.class));

        BroadcastStream<UserInfo> broadcastS2 = s2.broadcast(stateDescriptor);

        // 对连接流进行process处理, 用事先数据的打宽
        // 连接流1和广播流2
        SingleOutputStreamOperator<EventUserInfo> joinedResult = flattend.connect(broadcastS2)
            .process(new BroadcastProcessFunction<EventCount, UserInfo, EventUserInfo>() {
                // 主流处理
                @Override
                public void processElement(EventCount eventCount, BroadcastProcessFunction<EventCount, UserInfo, EventUserInfo>.ReadOnlyContext ctx, Collector<EventUserInfo> out) throws Exception {
                    ReadOnlyBroadcastState<Integer, UserInfo> broadcastState = ctx.getBroadcastState(stateDescriptor);
                    UserInfo userInfo = broadcastState.get(eventCount.getId());


                    // 关联成功的,输出到主流
                    if (userInfo != null) {
                        EventUserInfo eventUserInfo = new EventUserInfo();
                        eventUserInfo.setId(eventCount.getId());
                        eventUserInfo.setEventId(eventCount.getEventId());
                        eventUserInfo.setCnt(eventCount.getCnt());
                        eventUserInfo.setGender(userInfo.getGender());
                        eventUserInfo.setCity(userInfo.getCity());
                        out.collect(eventUserInfo);
                    } else {
                        // 关联失败的, 输出到侧流
                        ctx.output(cOutputTag, eventCount);
                    }
                }

                // 广播流处理
                @Override
                public void processBroadcastElement(UserInfo value, BroadcastProcessFunction<EventCount, UserInfo, EventUserInfo>.Context ctx, Collector<EventUserInfo> out) throws Exception {
                    // 把数据放入广播状态
                    BroadcastState<Integer, UserInfo> broadcastState = ctx.getBroadcastState(stateDescriptor);
                    broadcastState.put(value.getId(), value);
                }
            });

        // 3.对主流数据按性别分组, 取最大随机数所在的那一条数据作为输出结果
        SingleOutputStreamOperator<EventUserInfo> mainResult = joinedResult.keyBy(EventUserInfo::getGender)
            .reduce(new ReduceFunction<EventUserInfo>() {
                @Override
                public EventUserInfo reduce(EventUserInfo value1, EventUserInfo value2) throws Exception {
                    if (value1.getCnt() > value2.getCnt()) {
                        return value1;
                    } else {
                        return value2;
                    }
                }
            });

        mainResult.print("main");

        // TODO 主流输出到mysql
        SinkFunction<EventUserInfo> jdbcSink = JdbcSink.sink("insert into t_event_user values(?,?,?,?,?) on duplicate key update eventId = ? , cnt = ?, gender=?,city =?",
            new JdbcStatementBuilder<EventUserInfo>() {
                @Override
                public void accept(PreparedStatement stmt, EventUserInfo eventUserInfo) throws SQLException {
                    stmt.setInt(1, eventUserInfo.getId());
                    stmt.setString(2, eventUserInfo.getEventId());
                    stmt.setInt(3, eventUserInfo.getCnt());
                    stmt.setString(4, eventUserInfo.getGender());
                    stmt.setString(5, eventUserInfo.getCity());
                    stmt.setString(6, eventUserInfo.getEventId());
                    stmt.setInt(7, eventUserInfo.getCnt());
                    stmt.setString(8, eventUserInfo.getGender());
                    stmt.setString(9, eventUserInfo.getCity());
                }
            },
            JdbcExecutionOptions.builder()
                .withMaxRetries(3)
                .withBatchSize(1)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://localhost:3306/abc?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                .withUsername("root")
                .withPassword("123456")
                .build()
        );

        mainResult.addSink(jdbcSink);

        DataStream<EventCount> sideOutput = joinedResult.getSideOutput(cOutputTag);
        sideOutput.print("side");
        //// TODO 要开启checkpoint
        //ParquetWriterFactory<EventCount> eventCountParquetWriterFactory = ParquetAvroWriters.forReflectRecord(EventCount.class);
        //FileSink<EventCount> fileSink = FileSink.forBulkFormat(new Path("d:/tmp/"), eventCountParquetWriterFactory)
        //    .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH"))
        //    .withRollingPolicy(OnCheckpointRollingPolicy.build())
        //    .withOutputFileConfig(OutputFileConfig.builder().withPartSuffix(".parquet").withPartPrefix("doitedu").build())
        //    .build();

        //sideOutput.sinkTo(fileSink);


        env.execute();
    }
}
