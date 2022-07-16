package app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import utils.MyKafkaUtil;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 跳出率, page的消息
 * <p>
 * 输入json格式
 * {"common":{"ar":"230000","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone 8","mid":"mid_3","os":"iOS 13.2.3","uid":"36","vc":"v2.1.134"},
 * "page":{"during_time":8327,"item":"6,7","item_type":"sku_ids","last_page_id":"cart","page_id":"trade"},
 * "ts":1608267415000
 * }
 * <p>
 * 把满足单调的消息发送到kafka
 * <p>
 * 数据流: web/app -> Nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> Kafka(dwm)
 * <p>
 * 程 序: mockLog -> Nginx -> Logger.sh -> kafka(ZK) -> BaseLogApp -> Kafka -> UserJumpDetailApp -> Kafka
 *
 * @author malichun
 * @create 2022/07/11 0011 1:28
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 生产环境, 与Kafka分区数保持一致

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

        // TODO 2.读取Kafka主题的数据创建流
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        kafkaDS.print("dwd_page_log");

        // TODO 3.将每行数据转换为JSON对象并提取时间戳生成watermark
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject).assignTimestampsAndWatermarks(
            WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        // TODO 4.(CEP 1)定义模式序列
        // 第一个为空, 第二个一样的条件
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
                @Override
                public boolean filter(JSONObject value) throws Exception {
                    String lastPageId = value.getJSONObject("page").getString("last_page_id");
                    return lastPageId == null || lastPageId.length() <= 0;
                }
            }).next("next") // next 严格近邻, followedBy不是严格近邻
            .where(new SimpleCondition<JSONObject>() {
                @Override
                public boolean filter(JSONObject value) throws Exception {
                    String lastPageId = value.getJSONObject("page").getString("last_page_id");
                    return lastPageId == null || lastPageId.length() <= 0;
                }
            }).within(Time.seconds(10));

// !!!!上面的模式如果一样的情况下, 可以用循环模式来定义模式序列!!!!
        // 上面优化
        Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
                @Override
                public boolean filter(JSONObject value) throws Exception {
                    String lastPageId = value.getJSONObject("page").getString("last_page_id");
                    return lastPageId == null || lastPageId.length() <= 0;
                }
            })
            .times(2)
            .consecutive() // 指定严格近邻(next)
            .within(Time.seconds(10));
// !!!!上面的模式如果一样的情况下, 可以用循环模式来定义模式序列!!!!

        // TODO 5.(CEP 2)将模式序列作用到流上
        // 要区分不同用户
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // TODO 6.(CEP 3)提取事件(匹配上的和超时的)
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("timeOut") {
        };
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(
            timeOutTag,
            new PatternTimeoutFunction<JSONObject, JSONObject>() {
                @Override
                public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                    return map.get("start").get(0); // 流超时了, 取第一个
                }
            }, new PatternSelectFunction<JSONObject, JSONObject>() {
                @Override
                public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                    return map.get("start").get(0); // 两次事件, 都为null, 取第一个
                }
            });
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeOutTag);
        // TODO 7.UNION 两种事件
        DataStream<JSONObject> unionDS = timeOutDS.union(selectDS);

        // TODO 8.将数据写入Kafka
        unionDS.print();
        unionDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        // TODO 9.启动任务
        env.execute("UserJumpDetailApp");

    }

}
