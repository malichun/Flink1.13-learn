package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * mysql的cdc连接器使用测试
 *
 */
public class Demo14_MysqlCdcConnector {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint"); // 要开启checkpoint,否则实时数据消费不到


        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 建表
        tenv.executeSql("CREATE TABLE flink_score (\n" +
            "    id INT,\n" +
            "    name string,\n" +
            "    gender string,\n" +
            "    score double,\n" +
            "    primary key(id) not enforced\n" +
            ") WITH (\n" +
            "    'connector' = 'mysql-cdc',\n" +
            "    'hostname' = 'hadoop102',\n" +
            "    'port' = '3306',\n" +
            "    'username' = 'root',\n" +
            "    'password' = '123456',\n" +
            "    'database-name' = 'flinktest',\n" +
            "    'table-name' = 'score',\n" +
            "    'scan.startup.mode' ='initial'\n" +
            ")");

        //tenv.executeSql("select * from flink_score").print();

        // 按照性别划分求平均分
        tenv.executeSql("select gender,avg(score) as avg_score from flink_score group by gender")/* .print() */;




        // 建一个目标表，用来存放查询结果： 每种性别中，总分最高的前2个人
        tenv.executeSql("create table flink_rank(\n" +
            "    gender string, \n" +
            "    name string,\n" +
            "    score_amt double,\n" +
            "    rn bigint, \n" +
            "    primary key(gender,rn) not enforced\n" +
            ") with (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:mysql://hadoop102:3306/flinktest',\n" +
            "    'table-name' = 'score_rank',\n" +
            "    'username' = 'root',\n" +
            "    'password' = '123456'\n" +
            ")");



        tenv.executeSql("insert into flink_rank \n" +
            "select\n" +
            "    gender,\n" +
            "    name,\n" +
            "    score_amt,\n" +
            "    rn\n" +
            "from \n" +
            "(\n" +
            "    select\n" +
            "        gender,\n" +
            "        name,\n" +
            "        score_amt,\n" +
            "        row_number() over(partition by gender order by score_amt desc) as rn\n" +
            "    from \n" +
            "    (\n" +
            "        select\n" +
            "            gender,\n" +
            "            name,\n" +
            "            sum(score) as score_amt\n" +
            "        FROM\n" +
            "         flink_score\n" +
            "        group by gender,name\n" +
            "    ) o1\n" +
            ") o2\n" +
            "where rn <= 2");

    }
}
