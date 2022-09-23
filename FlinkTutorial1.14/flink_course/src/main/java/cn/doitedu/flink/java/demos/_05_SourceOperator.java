package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.LongValueSequenceIterator;
import org.apache.flink.util.NumberSequenceIterator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Arrays;
import java.util.List;

/**
 * 源测试
 *
 * @author malichun
 * @create 2022/08/01 0001 22:40
 */
public class _05_SourceOperator {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1); // 默认并行度

        DataStreamSource<Integer> fromElements = env.fromElements(1, 2, 3, 4, 5);
        fromElements.map(d -> d * 10)/* .print() */;

        //fromElements/* .print() */;

        List<String> dataList = Arrays.asList("a", "b", "a", "c");
        // fromCollection方法所返回的Source算子, 是一个但并行度的source算子, 尽管设置了并行度为5, 会抛异常, 只能有1个并行度
        DataStreamSource<String> fromCollectioin = env.fromCollection(dataList)/* .setParallelism(5) */;
        fromCollectioin.map(String::toUpperCase)/* .print() */;

        // fromParallelCollection所返回的Source算子, 是一个多并行度的source算子
        DataStreamSource<LongValue> parallelCollection = env.fromParallelCollection(new LongValueSequenceIterator(1, 100), TypeInformation.of(LongValue.class));
        parallelCollection.map(lv -> lv.getValue() + 100)/* .print() */;

        DataStreamSource<Long> sequence = env.generateSequence(1, 100);
        sequence.map(x -> x - 1)/* .print() */;

        DataStreamSource<String> fileSource = env.readTextFile("input/clicks.txt", "utf-8");
        fileSource.map(String::toUpperCase)/* .print() */;

        /// FileProcessingMode.PROCESS_ONCE 表示, 对文件只读一次, 计算一次, 然后程序就退出
        // FileProcessingMode.PROCESS_CONTINUOUSLY, 会件事这文件的变化, 一旦发现文件有变化, 则会再次会对整个文件进行重新计算
        env.readFile(new TextInputFormat(null), "input/clicks.txt", FileProcessingMode.PROCESS_ONCE, 1000);

        /**
         * 从socket 端口获取数据得到数据流
         */
        //env.socketTextStream("hadoop102",6666).print();


        /**
         * 从kafka中读取数据得到输出流
         *
         * 得到Source算子
         */
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setTopics("tp01")
            .setGroupId("gp01")
            .setBootstrapServers("hadoop102:9092")
            // OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST) 消费起始位移, 选择之前所提交的偏移量,(如果没有则重置为latest)
            // OffsetsInitializer.latest(): 从最新的数据开始消费
            // OffsetsInitializer.earliest(): 消费起始位移直接选择"最早"
            //OffsetsInitializer.offsets(Map<TopicPartition,Long>) 消费起始位置, 选择为方法所传入的每个分区和对应的起始偏移量
            // OffsetsInitializer.timestamp(Long timestamp) , 从指定的时间戳开始消费
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
            .setValueOnlyDeserializer(new SimpleStringSchema())

            // 把本Source算子 设置成 BOUNDED 属性(有界流), 将来本source去读取数据的时候, 读到指定的位置就停止读取并退出
            // 常用于补数或重跑某一段历史数据
            // .setBounded(OffsetsInitializer.committedOffsets())

            // 把本source算子设置成 UNBOUNDED 属性(无界流), 但是并不会一直读取数据, 而是达到指定位置就停止读取, 但程序不退出
            // 主要应用场景: 需要从kafka中读取某一段固定长度的数据, 然后拿着这段数据区另外一个真正无界流联合处理
            //    .setUnbounded(OffsetsInitializer.offsets())

            // 开启了kafka底层消费者的自动位移提交机制, 它会把最新的消费位移提交到Kafka的consumer_offset中
            // 就算把自动位移提交机制开启, KafkaSource依然不依赖自动位移提交机制(宕机重启时, 优先从flink自己的状态中取获取偏移量<更可靠>)
            .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
            .build();

        //env.addSource() // 接收的是 SourceFunction接口的实现类
        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk source");// 接收的是 Source 接口的实现类
        streamSource.print();


        env.execute();
    }
}
