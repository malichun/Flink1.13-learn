package com.atguigu.chapter05_StreamAPI;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;


/**
 * @author malichun
 * @create 2022/6/23 23:35
 */
public class Sink01ToFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4); // 并行度调整成4
        DataStreamSource<Event> stream = env.fromElements(
            new Event("Mary", "./home", 1000L),
            new Event("Bob", "./cart", 2000L),
            new Event("Alice", "./prod?id=100", 3000L),
            new Event("Bob", "./prod?id=1", 3500L),
            new Event("Alice", "./prod?id=200", 3200L),
            new Event("Bob", "./home", 3300L),
            new Event("Bob", "./prod?id=2", 3800L),
            new Event("Bob", "./prod?id=3", 4200L)
        );

        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"),
                new SimpleStringEncoder<>("UTF-8"))
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withMaxPartSize(1024 * 1024 * 1024) // 超过文件大小
                    .withRolloverInterval(TimeUnit.MINUTES.toMillis(15)) // 隔多长时间滚动生成
                    .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) // 不活跃的间隔时间
                    .build()
            ) // 生成文件策略
            .build();

        stream.map(Event::toString).addSink(streamingFileSink);


        env.execute();
    }
}
