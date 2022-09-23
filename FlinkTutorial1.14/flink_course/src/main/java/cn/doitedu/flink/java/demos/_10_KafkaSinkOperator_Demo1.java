package cn.doitedu.flink.java.demos;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author malichun
 * @create 2022/08/28 0028 1:05
 */
public class _10_KafkaSinkOperator_Demo1 {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        // 构造好一个数据流
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        // 把数据写入Kafka
        // 1.构造一个kafka的sink算子
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
            .setBootstrapServers("hadoop02:9092")
            .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                .setTopic("event-log")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // 投递语义
            .setTransactionalIdPrefix("doitedu-")
            .build();

        // 2. 把数据流输出到构造好的sink算子
        streamSource.map(JSON::toJSONString).disableChaining()
                .sinkTo(kafkaSink);

        streamSource.print();

        env.execute();
    }
}
