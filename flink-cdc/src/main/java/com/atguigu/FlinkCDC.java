package com.atguigu;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @author malichun
 * @create 2022/07/09 0009 15:14
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
       // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.1 开启Ck并指定后端为FS
        env.setStateBackend(new HashMapStateBackend());

        env.enableCheckpointing(5000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:9820/gmall-flink/ck");
        //// 开启检查点的外部持久化保存，作业取消后依然保留
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setCheckpointTimeout(10000L);
        checkpointConfig.setMaxConcurrentCheckpoints(2);
        checkpointConfig.setMinPauseBetweenCheckpoints(3000);

        // 配置重启策略, 新版本不用了
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.minutes(2)));

        System.setProperty("HADOOP_USER_NAME", "atguigu");

        env.setParallelism(1);
        // 2. 通过FlinkCDC构建SourceFunction,并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
            .hostname("hadoop102")
            .port(3306)
            .username("root")
            .password("123456")
            .databaseList("gmall2021")
            .tableList("gmall2021.base_trademark") //不加不添加该参数, 则消费指定数据库中所有的数据, 如果指定, 则指定方式为db.table
            .deserializer(new StringDebeziumDeserializationSchema())
            //.debeziumProperties() // 修改debeziumProperties
            .startupOptions(StartupOptions.initial()) // 会重新消费
            .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        //3. 打印数据
        streamSource.print();
        //4. 启动任务
        env.execute("FlinkCDC");
    }
}
