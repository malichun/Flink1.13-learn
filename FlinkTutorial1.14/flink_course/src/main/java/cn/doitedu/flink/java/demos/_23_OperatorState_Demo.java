package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 算子状态
 * @author malichun
 * @create 2022/09/12 0012 1:03
 */
public class _23_OperatorState_Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        source.map(new StatMapFunction()).print();

        env.execute();
    }
}

/**
 * 算子状态, 需要实现 CheckpointedFunction
 */
class StatMapFunction implements MapFunction<String, String>, CheckpointedFunction {

    ListState<String> listState ;

    @Override
    public String map(String value) throws Exception {

        return null;
    }

    /**
     * 系统对状态数据做快照(持久化)时会调用的方法, 用户可以利用这个方法, 在持久化前, 对状态数据做一些操控
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    /**
     * 算子任务, 在启动之初, 会调用下面的方法, 来为用户进行状态数据初始化
     * 类似于redis, 有各种数据结构
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 从方法提供的context中拿到一个算子状态存储器
        OperatorStateStore operatorStateStore = context.getOperatorStateStore();
        // 算子状态存储器, 只提供List数据结构来为用户存储数据
        // 定义一个状态存储描述器
        ListStateDescriptor<String> stateDescriptor = new ListStateDescriptor<>("strings", String.class);
        // 在状态存储器上调用get方法,得到具体的状态管理器 getListState方法, 他是会帮用户加载从程序重启之前所持久化的数据
        listState = operatorStateStore.getListState(stateDescriptor);

    }
}
