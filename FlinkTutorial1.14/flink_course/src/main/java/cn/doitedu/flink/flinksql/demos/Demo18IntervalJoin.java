package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author malichun
 * @create 2022/08/17 0017 23:24
 */
public class Demo18IntervalJoin {
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

        // interval join, 这个是往后join, a=4000, b= 4000 - 6000
        tenv.executeSql("select a.f0, a.f1,a.f2,b.f1 as l_rt, b.rt as r_rt " +
            "from t_left a join t_right b " +
            "on a.f0 = b.f0 " +
            "and a.rt between b.rt - interval '2' second and b.rt ");




    }
}
