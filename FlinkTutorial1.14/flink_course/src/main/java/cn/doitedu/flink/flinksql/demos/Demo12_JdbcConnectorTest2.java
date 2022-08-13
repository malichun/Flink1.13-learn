package cn.doitedu.flink.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * 作为sink
 * 往mysql表里面插入数据
 *
 * 测试join实现 +I -D +I 的操作
 *
 */
public class Demo12_JdbcConnectorTest2 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // id,gender
        SingleOutputStreamOperator<Bean1> bean1 = env
            .socketTextStream("hadoop102", 8888)
            .map(s -> {
                String[] arr = s.split(",");
                return new Bean1(Integer.parseInt(arr[0]), arr[1]);
            });
        // id,name
        SingleOutputStreamOperator<Bean2> bean2 = env
            .socketTextStream("hadoop102", 6666)
            .map(s -> {
                String[] arr = s.split(",");
                return new Bean2(Integer.parseInt(arr[0]), arr[1]);
            });


        tenv.createTemporaryView("bean1", bean1);
        tenv.createTemporaryView("bean2", bean2);




        // 建表来映射 mysql中的 flinktest.stu2, 作为sink表
        tenv.executeSql(" create table flink_stu2(                                 "
            + "     id int primary key not enforced,                    " // 要定义主键, 否则不支持撤回
            + "     name string,                                        "
            + "     gender string                                       "
            + " ) with (                                                "
            + "     'connector' = 'jdbc',                               "
            + "     'url' = 'jdbc:mysql://hadoop102:3306/flinktest',    "
            + "     'table-name' = 'stu2',                               "
            + "     'username' = 'root',                                "
            + "     'password' = '123456'                               "
            + " )                                                       ");

        // 从外部nc的数据, join后插入mysql表里面, 会产生 +I, -D, +I 的操作, (建Flink表需要设置主键, upsert sink)
        tenv.executeSql("insert into flink_stu2 " +
            "select bean1.id, bean1.gender, bean2.name from bean1 left join bean2 on bean1.id = bean2.id");

        // 这个只是append, 建flink表的时候可以不指定主键
        tenv.executeSql("insert into flink_stu2 select id,gender,'xxx' as name from bean1");

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
