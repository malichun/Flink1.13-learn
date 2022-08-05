package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * 理解Catalog
 */
public class Demo5_CatalogDemo {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","atguigu");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 环境创建之初, 底层会自动初始化一个 元数据空间实现对象( default_catalog => GenericInMemoryCatalog)
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 创建hive元数据的空间对象
        //连接 hive , hive的元空间, thrift server
        //  hive --service metastore , 9083端口 netstat -nltp | grep 9083, 配置只要thrift 的配置
        HiveCatalog hiveCatalog = new HiveCatalog("hive","default", "D:\\projects\\learn_projects\\flink-learn\\FlinkTutorial1.14\\flink_course\\src\\main\\resources");

        // 将Hive元数据空间对象注册到 环境中
        tenv.registerCatalog("mycatalog", hiveCatalog);


        // 将表建在hive元数据里面 // 永久表 FIXME !!!!!!!!!!!!!!!!!!!
        tenv.executeSql(
            " create temporary table `mycatalog`.`default`.`t_kafka`( "
                + "    id int,                                                "
                + "    name string,                                           "
                + "    age int,                                               "
                + "    gender string                                          "
                + ")  WITH (                                                  "
                + " 'connector' = 'kafka',                                    "
                + " 'topic' = 'doit30-2',                                     "
                + " 'properties.bootstrap.servers' = 'hadoop102:9092',        "
                + " 'properties.group.id' = 'g1',                             "
                + " 'format' = 'json',                                        "
                + " 'json.fail-on-missing-field' = 'false',                   "
                + " 'json.ignore-parse-errors' = 'true' ,                     "
                + " 'scan.startup.mode' = 'earliest-offset'                   "
                + ")                                                          "
        );


        tenv.executeSql(
            " create temporary table `t_kafka2`(                       "
                + "    id int,                                                "
                + "    name string,                                           "
                + "    age int,                                               "
                + "    gender string                                          "
                + ")  WITH (                                                  "
                + " 'connector' = 'kafka',                                    "
                + " 'topic' = 'doit30-2',                                     "
                + " 'properties.bootstrap.servers' = 'hadoop102:9092',        "
                + " 'properties.group.id' = 'g1',                             "
                + " 'format' = 'json',                                        "
                + " 'json.fail-on-missing-field' = 'false',                   "
                + " 'json.ignore-parse-errors' = 'true' ,                     "
                + " 'scan.startup.mode' = 'earliest-offset'                   "
                + ")                                                          "
        );

        // 基于表建个视图, 内存的空间, 没有指定元数据空间
        tenv.executeSql("create temporary view `mycatalog`.`default`.`t_kafka_view` as select id,name,age from mycatalog.`default`.t_kafka");


        String[] strings = tenv.listCatalogs();



        tenv.executeSql("show databases").print(); // default_database

        tenv.executeSql("use default_database");

        tenv.executeSql("show tables").print(); // t_kafka

        System.out.println("--------------------------");
// 连接hive
        tenv.executeSql("use catalog mycatalog");
        tenv.executeSql("show databases").print();
        tenv.executeSql("use `default`"); // hive的default库
        tenv.executeSql("show tables").print();

        // 设置hive方言
        tenv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tenv.executeSql("select * from dept").print();



    }
}
