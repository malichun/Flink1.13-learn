package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author malichun
 * @create 2022/08/15 0015 0:41
 */
public class Demo17_TimeWindowJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        /**
         *
         * 1,a,1000
         * 2,b,2000
         * 3,c,2500
         * 4,d,3000
         * 5,e,12000
         */
        SingleOutputStreamOperator<Tuple3<String,String,Long>> ss1 = env.socketTextStream("hadoop102", 6666)
            .map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0],arr[1], Long.parseLong(arr[2]));
        }).returns(new TypeHint<Tuple3<String, String, Long>>() {});


        /**
         * 1,bj,1000
         * 2,sh,2000
         * 4,xa,2600
         * 5,yn,12000
         */
        SingleOutputStreamOperator<Tuple3<String,String,Long>> ss2 = env.socketTextStream("hadoop102", 8888)
            .map(s -> {
                String[] arr = s.split(",");
                return Tuple3.of(arr[0],arr[1],Long.parseLong(arr[2]));
            }).returns(new TypeHint<Tuple3<String, String, Long>>() {});


        // 创建两个表
        tenv.createTemporaryView("t_left", ss1, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.STRING())
                .column("f2",DataTypes.BIGINT())
                .columnByExpression("rt", "to_timestamp_ltz(f2,3)")
                .watermark("rt","rt - interval '0' second")
            .build());

        tenv.createTemporaryView("t_right", ss2, Schema.newBuilder()
            .column("f0", DataTypes.STRING())
            .column("f1", DataTypes.STRING())
            .column("f2",DataTypes.BIGINT())
            .columnByExpression("rt", "to_timestamp_ltz(f2,3)")
            .watermark("rt","rt - interval '0' second")
            .build());


        // 各类窗口join示例
        // INNER
        tenv.executeSql("select\n" +
            "a.f0,a.f1,b.f0,b.f1,a.window_start,a.window_end\n" +
            "from \n" +
            "(\n" +
            "    SELECT\n" +
            "        *\n" +
            "    from \n" +
            "        table(tumble(table t_left, descriptor(rt), interval '10' second))\n" +
            ") a\n" +
            "join\n" +
            "(\n" +
            "    SELECT\n" +
            "        *\n" +
            "    from \n" +
            "        table(tumble(table t_right, descriptor(rt), interval '10' second))\n" +
            ") b\n" +
            "on a.window_start = b.window_start and a.window_end = b.window_end and a.f0 = b.f0"
        )/* .print() */;


        // LEFT / RIGHT / FULL JOIN
        tenv.executeSql("select\n" +
            "a.f0,a.f1,b.f0,b.f1,a.window_start,a.window_end\n" +
            "from \n" +
            "(\n" +
            "    SELECT\n" +
            "        *\n" +
            "    from \n" +
            "        table(tumble(table t_left, descriptor(rt), interval '10' second))\n" +
            ") a\n" +
            "full join\n" +
            "(\n" +
            "    SELECT\n" +
            "        *\n" +
            "    from \n" +
            "        table(tumble(table t_right, descriptor(rt), interval '10' second))\n" +
            ") b\n" +
            "on a.window_start = b.window_start and a.window_end = b.window_end and a.f0 = b.f0"
        )/* .print() */;

        // SEMI join  => where in
        tenv.executeSql("select\n" +
            "    a.f0, a.f1, a.f2,a.window_start,a.window_end\n" +
            "from \n" +
            "(\n" +
            "    select * from table(tumble(table t_left, descriptor(rt), interval '10' second))\n" +
            ") a\n" +
            "where a.f0 in \n" +
            "(\n" +
            "    select f0 from \n" +
            "    (\n" +
            "        select * from table(tumble(table t_right, descriptor(rt), interval '10' second))\n" +
            "    ) b\n" +
            "    where a.window_start = b.window_start and a.window_end = b.window_end\n" +
            ")").print();


        // ANTI join   => where not int
        tenv.executeSql(
            "select\n" +
            "    a.f0, a.f1, a.f2,a.window_start,a.window_end\n" +
            "from \n" +
            "(\n" +
            "    select * from table(tumble(table t_left, descriptor(rt), interval '10' second))\n" +
            ") a\n" +
            "where not a.f0 in \n" +
            "(\n" +
            "    select f0 from \n" +
            "    (\n" +
            "        select * from table(tumble(table t_right, descriptor(rt), interval '10' second))\n" +
            "    ) b\n" +
            "    where a.window_start = b.window_start and a.window_end = b.window_end\n" +
            ")").print();


    }
}
