package utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.annotation.Nullable;
import java.util.Properties;
import java.util.function.Function;

/**
 * @author malichun
 * @create 2022/07/09 0009 22:37
 */
public class MyKafkaUtil {
    private static String brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static String default_topic = "DWD_DEFAULT_TOPIC";

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic){
        return new FlinkKafkaProducer<String>(brokers,topic,new SimpleStringSchema());
    }


    /**
     * 根据消息内容指定topic
     * public FlinkKafkaProducer(
     *             String defaultTopic,
     *             KafkaSerializationSchema<IN> serializationSchema,
     *             Properties producerConfig,
     *             FlinkKafkaProducer.Semantic semantic)
     *
     */
    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        return new FlinkKafkaProducer<T>(
            default_topic,
            kafkaSerializationSchema, //
            properties,
            FlinkKafkaProducer.Semantic.EXACTLY_ONCE // 要开启checkpoint的时候才用EXA
        );
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(),properties);
    }
}
