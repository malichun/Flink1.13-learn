package cn.doitedu.flink.java.demos;

import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * 自定义的source算子
 * @author malichun
 * @create 2022/08/21 0021 20:53
 * @Desc: 自定义Source算子
 *
 * 自定义Source,
 *      可以实现 SourceFunction 或者 RichSourceFunction, 这两者都是非并行的Source算子
 *      也可实现 ParallelSourceFunction 或者 RichParallelSourceFunction, 这两者都是可并行的Source算子
 *
 * -- 带Rich的都拥有 open, close, getRuntimeContext方法
 * -- 带Parallel都可多实例并行执行
 *
 */
public class _06_CustomSourceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //DataStreamSource<EventLog> eventLogDataStreamSource = env.addSource(new MySourceFunction());
        DataStreamSource<EventLog> eventLogDataStreamSource = env.addSource(new MyRichSourceFunction());
        eventLogDataStreamSource.map(JSONObject::toJSONString).print();

        env.execute();
    }


}

@Data
@AllArgsConstructor
@NoArgsConstructor
class EventLog{
    private long guid;
    private String sessionId;
    private String eventId;
    private long timestamp;
    private Map<String,String> eventInfo;
}

/**
 * 我的source组件, 要产生的数据是 EventLog 对象
 */
class MySourceFunction implements SourceFunction<EventLog>{

    volatile boolean flag = true;

    String[] events = {"appLaunch", "pageLoad", "adShow", "adClick", "share", "itemCollect", "putBack", "wakeUp", "appClose"};

    Map<String, String> eventInfoMap = new HashMap<>();

    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {
        EventLog eventLog = new EventLog();
        while(flag){

            eventLog.setGuid(RandomUtils.nextLong(1, 1000));
            eventLog.setSessionId(RandomStringUtils.randomAlphabetic(1024).toUpperCase());
            eventLog.setTimestamp(System.currentTimeMillis());
            eventLog.setEventId(events[RandomUtils.nextInt(0, events.length)]);

            eventInfoMap.put(RandomStringUtils.randomAlphabetic(1), RandomStringUtils.randomAlphabetic(2));
            eventLog.setEventInfo(eventInfoMap);

            ctx.collect(eventLog);

            eventInfoMap.clear();

            Thread.sleep(RandomUtils.nextInt(5, 1500));
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}


class MyRichSourceFunction extends RichSourceFunction<EventLog>{

    volatile boolean flag = true;

    String[] events = {"appLaunch", "pageLoad", "adShow", "adClick", "share", "itemCollect", "putBack", "wakeUp", "appClose"};

    Map<String, String> eventInfoMap = new HashMap<>();

    /**
     * source组件初始化
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化的东西
        RuntimeContext runtimeContext = getRuntimeContext();

        // 可以从运行时上下文中取到本算子所属的task的task名
        String taskName = runtimeContext.getTaskName();

        // 可以从运行时上下文中欧, 取到本算子所属的subTask的subTaskId
        int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();

    }

    /**
     * source组件生成数据的过程(核心工作逻辑)
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {
        EventLog eventLog = new EventLog();
        while(flag){

            eventLog.setGuid(RandomUtils.nextLong(1, 1000));
            eventLog.setSessionId(RandomStringUtils.randomAlphabetic(12).toUpperCase());
            eventLog.setTimestamp(System.currentTimeMillis());
            eventLog.setEventId(events[RandomUtils.nextInt(0, events.length)]);

            eventInfoMap.put(RandomStringUtils.randomAlphabetic(1), RandomStringUtils.randomAlphabetic(2));
            eventLog.setEventInfo(eventInfoMap);

            ctx.collect(eventLog);

            eventInfoMap.clear();

            Thread.sleep(RandomUtils.nextInt(500, 1500));
        }
    }

    /**
     * job取消调用的方法
     */
    @Override
    public void cancel() {
        flag = false;
    }

    /**
     * 组件关闭调用的方法
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        System.out.println("组件被关闭了");
    }
}

// 并行的
 class MyParallelSourceFunction implements ParallelSourceFunction<EventLog> {

    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}

// 并行的
class MyRichParallelSourceFunction extends RichParallelSourceFunction<EventLog> {

    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
