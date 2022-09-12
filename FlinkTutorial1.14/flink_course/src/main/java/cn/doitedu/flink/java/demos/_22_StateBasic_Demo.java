package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author malichun
 * @create 2022/09/12 0012 1:03
 */
public class _22_StateBasic_Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // a
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        // 需要使用map算子来达到一个效果:
        // 每来一条数据(字符串), 都要输出该条字符串以及此前到达过的所有字符串
        source.map(new RichMapFunction<String, String>() {
            String acc = "";

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            /**
             * 要让flink来帮助管理的状态数据
             * ，那就不要自己定义一个变量
             * 而是要从flink的api中去获取一个状态管理器，用这个状态管理器来进行数据的增删改查等操作
             *
             * 这种状态： 叫做  托管状态 ！ (flink state)
             */
            @Override
            public String map(String value) throws Exception {
                acc += value;
                return acc;
            }
        }).print();

        env.execute();
    }
}
