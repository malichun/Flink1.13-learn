package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.stream.Stream;

/**
 * @author malichun
 * @create 2022/6/23 0:42
 */
public class Transform07PartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        // 1. shuffle 随机分区
        //stream.shuffle().print("shuffle").setParallelism(4);

        // 2. 轮询, roundRobin, 默认是轮询方式重分区
        //stream.rebalance().print("rebalance").setParallelism(4);

        // 3. 重缩放分区, rescale
        env.addSource(new RichParallelSourceFunction<Integer>() {

                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        for (int i = 1; i <= 8; i++) {
                            // 将奇偶数分别发送到0号和1号并行分区
                            if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                                ctx.collect(i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                }).setParallelism(2)
                //.rescale()
                //.print()
                .setParallelism(4);


        // 4. 广播, 数据复制并发送到下游所有并行任务中
        //stream.broadcast().print().setParallelism(4);

        // 5.全局分区
        //stream.global().print().setParallelism(5); // 全部都分配到一个分区中

        // 6.自定义重分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                })
                .print().setParallelism(4);

        env.execute();
    }
}
