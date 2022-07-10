package com.atguigu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author malichun
 * @create 2022/07/09 0009 16:31
 */
public class FlinkSqlCDC {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //2.创建 Flink-MySQL-CDC 的 Source
        tableEnv.executeSql("CREATE TABLE mysql_binlog (\n" +
            " id STRING NOT NULL,\n" +
            " tm_name STRING,\n" +
            " logo_url STRING\n" +
            ") WITH (\n" +
            " 'connector' = 'mysql-cdc',\n" +
            " 'hostname' = 'hadoop102',\n" +
            " 'port' = '3306',\n" +
            " 'username' = 'root',\n" +
            " 'password' = '123456',\n" +
            " 'database-name' = 'gmall2021',\n" +
            " 'table-name' = 'base_trademark'\n" +
            ")");

        // 3. 查询数据
        Table table = tableEnv.sqlQuery("select * from mysql_binlog");


        //4 .将数据转换成流
        tableEnv.toChangelogStream(table).print();

        // 5.启动任务
        env.execute();

    }
}
