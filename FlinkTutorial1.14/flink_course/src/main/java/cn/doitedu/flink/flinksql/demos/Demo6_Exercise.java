package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * 基本: kafak中有如下数据
 * {"id":1, "name":"zs","nick":"tiedan","age":18,"gender":"male"}
 *  kafka中有如下数据:
 * {"id":1,"name":{"formal":"zs","nick":"tiedan"}, "age":18, "gender":"male"}
 *
 * 需要用flink sql来对数据进行查询统计
 *  截止到当前, 每个昵称有多少用户?
 *  截止到当前, 每个性别, 年龄最大值?
 *
 */
public class Demo6_Exercise {
    public static void main(String[] args) {

        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        // 建表(数据源表)
        tenv.executeSql(
            "create table t_person (                         \n" +
            "    id int,                                               \n" +
            "    name string,                                          \n" +
            "    nick string,                                          \n" +
            "    age int,                                              \n" +
            "    gender string                                         \n" +
            ")  WITH (                                                 \n" +
            " 'connector' = 'kafka',                                   \n" +
            " 'topic' = 'doit30-4',                                    \n" +
            " 'properties.bootstrap.servers' = 'hadoop102:9092',       \n" +
            " 'properties.group.id' = 'g1',                            \n" +
            " 'format' = 'json',                                       \n" +
            " 'json.fail-on-missing-field' = 'false',                  \n" +
            " 'json.ignore-parse-errors' = 'true' ,                    \n" +
            " 'scan.startup.mode' = 'earliest-offset'                  \n" +
            ")  ");

        // 创建目标表
        // kafka 连接器, 不能接受 UPDATE 修正模式的数据, 只能接收INSERT模式的数据
        // 而我们的查询语句产生的结果, 存在UPDATE模式, 就需要另一种连接器表(upsert-kafka)来接收
        tenv.executeSql(
            "create table t_nick_cnt (                         \n" +
                "    nick string,                                          \n" +
                "    user_cnt bigint," +
                "    primary key (nick) not enforced                 \n" + // 要指定主键
                ")  WITH (                                                 \n" +
                " 'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'doit30-nick',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")  ");


        // 查询 并 打印
        // TableResult tableResult = tenv.executeSql("select nick, count(distinct id) as user_cnt from t_person group by nick");

        // 输出
        tenv.executeSql("insert into t_nick_cnt " +
            "select nick,count(distinct id) as user_cnt from t_person group by nick");




    }
}
