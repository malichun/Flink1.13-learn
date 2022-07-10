package app.dwd;

import app.function.CustomerDebeziumDeserialization;
import app.function.DimSinkFunction;
import app.function.TableProcessFunction;
import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import utils.MyKafkaUtil;

import javax.annotation.Nullable;

/**
 *  使用广播流方式通知表类型
 *  数据流 : web/app -> nginx -> Springboot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp(dwd) -> Kafka/Phoenix(dim)
 *  程 序 :           mockDb               -> MySql -> FlinkCDC -> Kafka(ZK) -> BaseDBApp  -> Kafka/Phoenix(hbase,zk,hdfs)
 *
 * @author malichun
 * @create 2022/07/10 0010 11:01
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
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

        // TODO 2. 消费kafka ods_base_db 主题数据创建流
        String sourceTopic = "ods_base_db";
        String groupId = "base_db_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // TODO 3. 将每行数据转换为json对象, 并过滤(delete数据) 主流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
            .filter(new FilterFunction<JSONObject>() {
                @Override
                public boolean filter(JSONObject value) throws Exception {
                    // 取出数据的操作类型
                    String type = value.getString("type");
                    return !"delete".equals(type);
                }
            });

        // TODO 4. 使用FlinkCDC消费配置表,并处理成            广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
            .hostname("hadoop102")
            .port(3306)
            .username("root")
            .password("123456")
            .databaseList("gmall2021_realtime") // 一般放自己的库,不放业务库
            .tableList("gmall2021_realtime.table_process") // 要监控的表
            .startupOptions(StartupOptions.initial()) // 初始化
            .deserializer(new CustomerDebeziumDeserialization()) // 序列化
            .build();

        DataStreamSource<String> tableProcessStrDS = env.addSource(sourceFunction);
        tableProcessStrDS.print("table_process source: ");

        // 联合主键 表名+操作类型
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);

        // TODO 5. 连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);


        // TODO ***6. 分流 处理数据 广播流数据, 主流数据(根据广播流数据进行处理)  // 泛型<IN1,IN2,OUT>
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {
        };
        SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

        // TODO 7. 提取Kafka流数据和HBase流数据
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);
        // TODO ***8. 将Kafka数据写入Kafka主题, 将HBase数据写入Phoenix表
        kafka.print("kafka");
        hbase.print("hbase");

        // jdbcSink
        hbase.addSink(new DimSinkFunction());

        // kafkaSink
        // 消息体: {"sinkTable":"dwd_order_info","database":"gmall2021","before":{},"after":{"user_id":4000,"id":26449},"type":"insert","tableName":"order_info"}
        //kafka.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaProducer("")) // 用不了
        kafka.addSink(MyKafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<>(
                    element.getString("sinkTable"),
                    element.getString("after").getBytes()
                    );
            }
        }));

        // TODO 9. 启动任务
        env.execute("BaseDBApp");
    }
}
