package com.atguigu.chapter05_StreamAPI;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author malichun
 * @create 2022/6/24 0:02
 */
public class Sink02ToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从kafka中读取数据
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop103:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // 还有两个反序列化的配置

        DataStreamSource<String> kafkaStreamSource = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), props));

        // 2. 用Flink进行转换处理
        SingleOutputStreamOperator<String> result = kafkaStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Event(arr[0].trim(), arr[1].trim(), Long.parseLong(arr[2].trim())).toString();
            }
        });

        // 3.结果数据写入kafka
        result.addSink(new FlinkKafkaProducer<String>("hadoop102:9092,hadoop103:9092,hadoop103:9092", "events", new SimpleStringSchema()));
        env.execute();
    }
}
