package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 测试写数据
 * @author malichun
 * @create 2022/08/13 0013 17:05
 */
public class Demo13_FileSystemConnectorTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        tenv.executeSql("CREATE TABLE fs_table (\n" +
            "  user_id STRING,\n" +
            "  order_amount DOUBLE,\n" +
            "  dt STRING,\n" +
            "  `hour` STRING\n" +
            ") PARTITIONED BY (dt, `hour`) WITH (\n" +
            "  'connector'='filesystem',\n" +
            "  'path'='file:///d:/filetable',\n" +
            "  'format'='json',\n" +
            "  'sink.partition-commit.delay'='1 h',\n" +
            "  'sink.partition-commit.policy.kind'='success-file',\n" +
            "  'sink.rolling-policy.file-size' = '8M',\n" +
            "  'sink.rolling-policy.rollover-interval' = '30 min',\n" +
            "  'sink.rolling-policy.check-interval' = '1 min'\n" +
            ")");


        SingleOutputStreamOperator<Tuple4<String, Double, String, String>> stream = env.socketTextStream("hadoop102", 8888)
            .map(s -> {
                String[] arr = s.split(",");
                return Tuple4.of(arr[0], Double.valueOf(arr[1]), arr[2], arr[3]);
            })
            .returns(TypeInformation.of(new TypeHint<Tuple4<String, Double, String, String>>() {}));

        tenv.createTemporaryView("orders", stream);

        tenv.executeSql("insert into fs_table select * from orders");
    }

}
