package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author malichun
 * @create 2022/08/17 0017 1:45
 */
public class Demo19_LookupJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        Configuration configuration = tenv.getConfig().getConfiguration();

        // 设置table环境中的状态ttl时长
        configuration.setLong("table.exec.state.ttl", 60 * 60 * 1000); // 设置状态过期时间

        /**
         *
         * 1,a
         * 2,b
         * 3,c
         * 4,d
         * 5,e
         */
        SingleOutputStreamOperator<Tuple2<Integer,String>> ss1 = env.socketTextStream("hadoop102", 6666)
            .map(s -> {
                String[] arr = s.split(",");
                return Tuple2.of(Integer.valueOf(arr[0]),arr[1]);
            }).returns(new TypeHint<Tuple2<Integer, String>>() {});

        // 创建主表, 需要声明处理时间属性字段
        tenv.createTemporaryView("a", ss1, Schema.newBuilder()
            .column("f0", DataTypes.INT())
            .column("f1", DataTypes.STRING())
            .columnByExpression("proc_time", "PROCTIME()")
            .build());

        tenv.executeSql("desc a").print();

        // 创建lookup表(jdbc connector)
        tenv.executeSql(" create table b(                                 "
            + "     id int primary key not enforced,                    "
            + "     name string,                                        "
            + "     gender string                                       "
            + " ) with (                                                "
            + "     'connector' = 'jdbc',                               "
            + "     'url' = 'jdbc:mysql://hadoop102:3306/flinktest',    "
            + "     'table-name' = 'stu2',                               "
            + "     'username' = 'root',                                "
            + "     'password' = '123456',                               "
            + "     'lookup.cache.ttl' = '24 hour',                     "
            + "     'lookup.cache.max-rows' = '100'                   "
            + " )                                                       ");

        // lookup join 查询
        tenv.executeSql("select a.*,c.* from a join b FOR SYSTEM_TIME AS OF a.proc_time AS c on a.f0 = c.id").print();

    }
}
