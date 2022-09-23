package cn.doitedu.flink.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * upsert kafka
 *
 * 利用聚合操作实现 +I, -U +U
 */
public class Demo11_UpsertKafkaConnectorTest {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082);
        configuration.setString(RestOptions.BIND_PORT, "8081-8089");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 1,male
        DataStreamSource<String> s1 = env.socketTextStream("hadoop102", 8888);


        SingleOutputStreamOperator<Bean1> bean1 = s1.map(s -> {
            String[] arr = s.split(",");
            return new Bean1(Integer.parseInt(arr[0]), arr[1]);
        });

        tenv.createTemporaryView("bean1", bean1);

        tenv.executeSql("desc bean1").print();

        // tenv.executeSql("select gender,count(1) as cnt from bean1 group by gender").print();

        // 建一张upsert kafka表, 插入更新的数据
        tenv.executeSql(" create table t_upsert_kafka(                               "
                + "     gender string primary key not enforced,                "
                + "     cnt bigint                                             "
                + " ) with (                                                   "
                + "     'connector' = 'upsert-kafka',                          "
                + "     'topic' = 'doit30-upsert',                             "
                + "     'properties.bootstrap.servers' = 'hadoop102:9092',     "
                + "     'key.format' = 'csv',                                  "
                + "     'value.format' = 'csv'                                 "
                + " )                                                          "
        );

        // 查询每种性别的数据行数, 并将结果插入到目标表
        tenv.executeSql("insert into t_upsert_kafka select gender,count(1) as cnt from bean1 group by gender");

        // 读取目标表的数据
        tenv.executeSql("select * from t_upsert_kafka").print();

        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Bean1{
        public int id;
        public String gender;
    }

}
