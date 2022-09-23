package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author malc
 * @create 2022/9/20 0020 9:26
 */
public class _24_State_KeyedStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // 开启状态数据的checkpoint机制（快照的周期，快照的模式）, 不传默认是EXACTLY_ONCE
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        // 开启快照后，就需要指定快照数据的持久化存储位置
        /* env.getCheckpointConfig().setCheckpointStorage(new URI("hdfs://doit01:8020/checkpoint/"));*/
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint/");


        // 开启  task级别故障自动 failover
        // env.setRestartStrategy(RestartStrategies.noRestart()); // 默认是，不会自动failover；一个task故障了，整个job就失败了
        // 使用的重启策略是： 固定重启上限和重启时间间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000));


        DataStreamSource<String> source = env.socketTextStream("hadoop02", 9999);


        // 需要使用map算子来达到一个效果
        // 每来一条数据(字符串), 输出 该字符串中拼接此前到达的所有字符串
        source.keyBy(s -> s)
            .map(new RichMapFunction<String, String>() {

                ListState<String> listState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    RuntimeContext runtimeContext = getRuntimeContext();

                    // 获取一个List结构的状态存储器
                    listState = runtimeContext.getListState(new ListStateDescriptor<String>("list", String.class));

                    // 获取一个单值 结构的状态存储器
                    //runtimeContext.getState()


                    // 获取一个Map结构的状态存储器
                    //runtimeContext.getMapState()
                }

                @Override
                public String map(String value) throws Exception {
                    // 将本条数据装入状态存储器
                    listState.add(value);

                    // 遍历所有历史字符串, 拼接结果
                    Iterable<String> strings = listState.get();
                    StringBuilder sb = new StringBuilder();
                    for(String s: strings){
                        sb.append(s);
                    }
                    return sb.toString();
                }
            })
            .setParallelism(2)
            .print().setParallelism(2);

        // 提交一个job
        env.execute();
    }
}

// 输入:
//a
//b
//a
//b
//c
//a

// 输出
//2> a
//1> b
//2> aa
//1> bb
//1> c
//2> aaa
