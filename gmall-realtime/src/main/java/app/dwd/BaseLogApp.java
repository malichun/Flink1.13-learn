package app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import utils.MyKafkaUtil;

import java.text.ParseException;


/**
 * 分流
 *  数据流: web/app -> Nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd)
 *
 *  程 序: mockLog -> Nginx -> Logger.sh -> kafka(ZK) -> BaseLogApp -> Kafka
 *
 * @author malichun
 * @create 2022/07/09 0009 23:37
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.1 设置CK和状态后端
        //env.setStateBackend(new HashMapStateBackend());
        //env.enableCheckpointing(5000);
        //CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //checkpointConfig.setCheckpointStorage("hdfs://hadoop102:9820/gmall-flink/ck");
        ////// 开启检查点的外部持久化保存，作业取消后依然保留
        //checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //checkpointConfig.setCheckpointTimeout(10000L);
        //checkpointConfig.setMaxConcurrentCheckpoints(2);
        //checkpointConfig.setMinPauseBetweenCheckpoints(3000);

        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 2.消费 ods_base_log 主题数据, 创建流
        String groupId = "ods_dwd_base_log_app";
        String sourceTopic = "ods_base_log";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // TODO 3.将每行数据转换为JSON对象
        OutputTag<String> outputTag = new OutputTag<String>("Dirty") {
        };

        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() { // <I,O>
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    // 发生异常, 将数据写入侧输出流
                    ctx.output(outputTag, value);
                }
            }
        });

        // 打印脏数据
        jsonObjectDS.getSideOutput(outputTag).print("Dirty>>>>");

        // TODO 4.做新老用户的校验 (状态编程)
        // 先根据mid分组 mid = common-> mid
        SingleOutputStreamOperator<JSONObject> jsonObjectWithNewFlagDS = jsonObjectDS
            .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
            .map(new RichMapFunction<JSONObject, JSONObject>() { // <I,O>
                // 定义状态 有一部分数据的1要变为0
                private ValueState<String> valueState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
                }

                @Override
                public JSONObject map(JSONObject value) throws Exception {
                    // 获取数据中的"is_new"标记
                    String isNew = value.getJSONObject("common").getString("is_new");
                    // 判断isNew 标记是否为1
                    if ("1".equals(isNew)) {
                        // 获取状态数据
                        String state = valueState.value();
                        if (state != null) {
                            // 修改isNew标记
                            value.getJSONObject("common").put("is_new", "0");
                        } else {
                            valueState.update("1");
                        }

                    }
                    return value;
                }
            });

        // TODO 5.分流, 使用侧输出流 页面:主流  启动:侧输出流  曝光:侧输出流
        OutputTag<String> startOutputTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displaysOutputTag = new OutputTag<String>("display") {
        };

        SingleOutputStreamOperator<String> pageDs = jsonObjectWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() { // <I,O>

            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                // 启动日志字段
                String start = value.getString("start");
                if (start != null && start.length() > 0) {
                    // 将数据写入启动日志侧输出流
                    ctx.output(startOutputTag, value.toJSONString());
                } else {
                    // 将数据写入页面日志, 主流
                    out.collect(value.toJSONString());

                    // 取出数据中的曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        // 获取页面id
                        String pageId = value.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            // 添加页面id, 添加公共字段
                            display.put("page_id", pageId);
                            // /将数据写出到曝光侧输出流
                            ctx.output(displaysOutputTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        // TODO 6.提取侧输出流数据
        DataStream<String> startDS = pageDs.getSideOutput(startOutputTag);
        DataStream<String> displayDS = pageDs.getSideOutput(displaysOutputTag);

        // TODO 7.将3个流进行打印并输出到对应kafka主题中
        pageDs.print("page>>>>>>>>>>>>>>>>");
        startDS.print("start>>>>>>>>>>>>>>>>");
        displayDS.print("display>>>>>>>>>>>");

        pageDs.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        // TODO 8.启动任务
        env.execute("BaseLogApp");

    }
}
