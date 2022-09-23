package cn.doitedu.flink.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * upsert kafka
 * 测试 -D +I 使用join来操作
 */
public class Demo11_UpsertKafkaConnectorTest2 {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082);
        configuration.setString(RestOptions.BIND_PORT, "8081-8089");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 1,male
        DataStreamSource<String> s1 = env.socketTextStream("hadoop102", 8888);
        // 1,zs
        DataStreamSource<String> s2 = env.socketTextStream("hadoop102", 6666);


        SingleOutputStreamOperator<Bean1> bean1 = s1.map(s -> {
            String[] arr = s.split(",");
            return new Bean1(Integer.parseInt(arr[0]), arr[1]);
        });

        SingleOutputStreamOperator<Bean2> bean2 = s2.map(s -> {
            String[] arr = s.split(",");
            return new Bean2(Integer.parseInt(arr[0]), arr[1]);
        });

        tenv.createTemporaryView("bean1", bean1);
        tenv.createTemporaryView("bean2", bean2);



        // tenv.executeSql("select gender,count(1) as cnt from bean1 group by gender").print();

        // 建一张upsert kafka表, 插入更新的数据
        tenv.executeSql(
            " create table t_upsert_kafka2(                               "
                + "     id int primary key not enforced,                       "
                + "     gender  string ,                                       "
                + "     name string                                            "
                + " ) with (                                                   "
                + "     'connector' = 'upsert-kafka',                          "
                + "     'topic' = 'doit30-upsert2',                             "
                + "     'properties.bootstrap.servers' = 'hadoop102:9092',     "
                + "     'key.format' = 'csv',                                  "
                + "     'value.format' = 'csv'                                 "
                + " )                                                          "
        );

        // 查询每种性别的数据行数, 并将结果插入到目标表
        tenv.executeSql("insert into t_upsert_kafka2 " +
            " select bean1.id,bean1.gender, bean2.name from bean1 left join bean2 on bean1.id = bean2.id");

        // 读取目标表的数据
        tenv.executeSql("select * from t_upsert_kafka2").print();

        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Bean1{
        public int id;
        public String gender;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Bean2{
        public int id;
        public String name;
    }

}
