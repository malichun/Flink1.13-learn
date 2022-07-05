package com.atguigu.chapter11_table;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author malichun
 * @create 2022/07/04 0004 22:53
 */
public class Test002_commonApiTest {
    public static void main(String[] args) throws Exception {
        //// 获取流执行环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        //
        //// 默认是 BlinkPlaner和流处理模式
        //StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 1. 定义环境配置来创建表指定环境, 定义方式2, 不依赖env
        // 基于blink版本的计划器进行流处理
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
            .inStreamingMode()  // 默认
            .useBlinkPlanner() // 默认
            .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);


        // 1.1 基于老版本计划器进行 流 处理
        EnvironmentSettings settings1 = EnvironmentSettings.newInstance()
            .inStreamingMode()
            .useOldPlanner()
            .build();
        TableEnvironment tableEnv1 = TableEnvironment.create(settings1);

        // 1.2 基于老版本planner进行 批 处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(batchEnv);


        // 1.3 基于blink版本planner进行 批 处理
        EnvironmentSettings settings3 = EnvironmentSettings.newInstance()
            .inBatchMode()
            .useBlinkPlanner()
            .build();

        TableEnvironment tableEnv3 = TableEnvironment.create(settings3);

        // 2. 创建表(连接器表)
        String createDDL = "CREATE TABLE clickTable(" +
            " user_name STRING, " +
            " url STRING," +
            " ts bigint" +
            ") WITH (" +
            " 'connector' = 'filesystem'," +
            " 'path' = 'input/clicks.txt'," +
            " 'format' = 'csv'" +
            " )";
        tableEnv.executeSql(createDDL);

        // 创建一张用于输出的表
        String createOutDDL = "CREATE TABLE outTable(" +
            " url STRING, " +
            " user STRING " +
            ") WITH (" +
            " 'connector' = 'filesystem'," +
            " 'path' = 'output'," + // 输出目录
            " 'format' = 'csv'" +
            " )";

        tableEnv.executeSql(createOutDDL);




    }
}
