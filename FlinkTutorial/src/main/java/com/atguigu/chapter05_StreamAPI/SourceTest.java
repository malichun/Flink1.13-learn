package com.atguigu.chapter05_StreamAPI;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author malichun
 * @create 2022/6/19 22:37
 */
public class SourceTest {
    public static void main(String[] args) throws Exception{
        // 创建执行对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从文件直接读取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

        // 2. 从集合中读取数据
        ArrayList<Integer> numbers = new ArrayList<Integer>(){{
            add(2);
            add(5);
        }};
        DataStreamSource<Integer> numStream = env.fromCollection(numbers);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);

        // 3. 从元素读取数据
        DataStreamSource<Object> stream3 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L));

        // 4. 从socket文本流读取
        DataStreamSource<String> stream4 = env.socketTextStream("hadoop102", 9999);

        // 5. 从kafka中读取数据
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop103:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // 还有两个反序列化的配置

        DataStreamSource<String> kafkaStreamSource = env.addSource(new FlinkKafkaConsumer<String>("first", new SimpleStringSchema(), props));

        stream1.print("1");
        numStream.print("nums");
        stream2.print("2");
        stream3.print("3");
        stream4.print("4");

        kafkaStreamSource.print("kafka");


        env.execute();

    }
}
