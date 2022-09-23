package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * json format详解
 * <p>
 * {"id":10, "name":{"nick":"doe1", "formal":"doit edu1"}}
 * {"id":11, "name":{"nick":"doe2", "formal":"doit edu2"}}
 * {"id":12, "name":{"nick":"doe3", "formal":"doit edu3"}}
 *
 * 总结:
 *  map获取元素的方式两种
 *      1. name.nick
 *      2. name['nick']
 *  array 获取元素的方式
 *      array[0]
 *
 * @author malichun
 * @create 2022/08/09 0009 23:36
 */
public class Demo8_JsonFormat {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // TODO 1. 复杂对象, 使用sql的方式解析获取
        tenv.executeSql(
            "create table t_json1(                                 "
            + "    id int,                                           "
            + "    name map<string,string>,                          "
            + "    big_id as id * 10                                 "
            //+ "   filename string not null metadata from 'file.name'"  // -- filesystem 元数据字段, 1.15版才支持
            + " "
            + ") with (                                              "
            + "'connector' = 'filesystem',                           "
            + "'path' = 'FlinkTutorial1.14\\flink_course\\data\\sqldemo\\person.txt',                                          "
            + "'format' = 'json'                                     "
            + ")                                                     "
        );
        tenv.executeSql("desc t_json1").print();

        tenv.executeSql("select * from t_json1")/* .print() */;

        // 查询每隔人的id和nick
        tenv.executeSql("select id, name['nick'] as nick from t_json1")/* .print() */;

        System.out.println("========================================");

        // TODO 2.使用 TABLE API 的方式定义表, 复杂对象
        // {"id":11, "name":{"nick":"doe2", "formal":"doit edu2", "height": 170}}
        tenv.createTable("t_json2", TableDescriptor.forConnector("filesystem")
                .format("json")
                .schema(Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.ROW(
                        DataTypes.FIELD("nick", DataTypes.STRING()),
                        DataTypes.FIELD("formal", DataTypes.STRING()),
                        DataTypes.FIELD("height", DataTypes.INT())
                    ))
                    .build())
                .option("path","FlinkTutorial1.14\\flink_course\\data\\sqldemo\\qiantao2.txt")
            .build());


        tenv.executeSql("desc t_json2")/* .print() */;

        tenv.executeSql("select * from t_json2")/* .print() */;

        // 查询每隔人的id, formal, height
        tenv.executeSql("select id, name['formal'] as formal, name.height from t_json2")/* .print() */;
        /*
        +----+-------------+--------------------------------+-------------+
        | op |          id |                         formal |      height |
        +----+-------------+--------------------------------+-------------+
        | +I |          10 |                      doit edu1 |         180 |
        | +I |          11 |                      doit edu2 |         170 |
        | +I |          12 |                      doit edu3 |         175 |
        +----+-------------+--------------------------------+-------------+
         */

        // TODO 3.解析数组
        //{"id":1,"friends":[{"name":"a","info":{"addr":"bj","gender":"male"}},{"name":"b","info":{"addr":"sh","gender":"female"}}]}
        // FIXME 记得用row类型(类似 hive中的struct类型)
        tenv.executeSql("create table t_json3(                                                "
                + "    id int,                                                              "
                + "    friends array<row<name string, info map<string, string>>>            "
                + ") with (                                                                 "
                + "'connector' = 'filesystem',                                              "
                + "'path' = 'FlinkTutorial1.14\\flink_course\\data\\sqldemo\\qiantao3.txt', "
                + "'format' = 'json'                                                        "
                + ")                                                                        "
        );

        tenv.executeSql("desc t_json3").print();
        tenv.executeSql("select * from t_json3").print();

        // 所有字段展平 flatMap
        tenv.executeSql("select \n" +
            "    id,\n" +
            "    friends[1].name as name1,\n" +
            "    friends[1].info['addr'] as addr1,\n" +
            "    friends[1].info['gender'] as gender1,\n" +
            "    friends[2].name as name2,\n" +
            "    friends[2].info['addr'] as addr2,\n" +
            "    friends[2].info['gender'] as gender2\n" +
            "from \n" +
            "    t_json3").print();

        // TODO 使用格式
        //tenv.createTable("t_json3", TableDescriptor.forConnector("filesystem")
        //        .format("json")
        //        .schema(Schema.newBuilder()
        //
        //            .build())
        //        .option("path", "FlinkTutorial1.14\\flink_course\\data\\sqldemo\\qiantao3.txt")
        //    .build());

    }
}
