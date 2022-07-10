package app.ods;

import app.function.CustomerDebeziumDeserialization;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import utils.MyKafkaUtil;

import java.util.Properties;

/**
 * @author malichun
 * @create 2022/07/09 0009 22:29
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.1 设置CK和状态后端
        //env.setStateBackend(new HashMapStateBackend());
        //env.enableCheckpointing(5000);
        //CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //checkpointConfig.setCheckpointStorage("hdfs://hadoop102:9820/gmall-flink/ck");
        ////// 开启检查点的外部持久化保存，作业取消后依然保留
        //checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //checkpointConfig.setCheckpointTimeout(10000L);
        //checkpointConfig.setMaxConcurrentCheckpoints(2);
        //checkpointConfig.setMinPauseBetweenCheckpoints(3000);

        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // 2. 通过FlinkCDC构建SourceFunction,并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
            .hostname("hadoop102")
            .port(3306)
            .username("root")
            .password("123456")
            .databaseList("gmall2021")
            //.tableList("gmall2021.base_trademark") //不加不添加该参数, 则消费指定数据库中所有的数据, 如果指定, 则指定方式为db.table
            .deserializer(new CustomerDebeziumDeserialization())
            //.debeziumProperties() // 修改debeziumProperties
            .startupOptions(StartupOptions.latest())
            .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        //3. 将数据写入kafka
        String sinkTopic = "ods_base_db";
        streamSource.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));
        streamSource.print("业务数据");
        //4. 启动任务
        env.execute("FlinkCDC");
    }
}
