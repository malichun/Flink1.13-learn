package app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.MyKafkaUtil;

import java.text.SimpleDateFormat;

/**
 * 功能: 每个mid(用户),每天只会发送最早的一条数据进入dwm_unique_visit 主题, 没有做uv 求count
 * dwm只做去重, 往下游发送
 *
 *数据流: web/app -> Nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> Kafka(dwm)
 *程 序: mockLog -> Nginx -> Logger.sh -> kafka(ZK) -> BaseLogApp -> Kafka(dwd_page_log主题)    -> UniqueVisitApp -> Kafka
 *
 * UV测试数据
 *  {"common":{"ar":"230000","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone 8","mid":"mid_3","os":"iOS 13.2.3","uid":"36","vc":"v2.1.134"},
 *   "page":{"during_time":8327,"item":"6,7","item_type":"sku_ids","last_page_id":"cart","page_id":"trade"},
 *   "ts":1608267415000
 *   }
 *
 * @author malichun
 * @create 2022/07/10 0010 23:34
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 获取执行环境
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

        // TODO 2. 读取Kafka dwd_page_log 主题的数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // TODO 3. 将每行数据转换为JSON对象
        // 日志格式:
        // {"common":{"ar":"230000","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone 8","mid":"mid_3","os":"iOS 13.2.3","uid":"36","vc":"v2.1.134"},
        //    "page":{"during_time":8327,"item":"6,7","item_type":"sku_ids","last_page_id":"cart","page_id":"trade"},
        //    "ts":1608267415000
        //    }
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.map(JSON::parseObject);

        // TODO 4. 过滤数据 状态编程 只保留每个mid每天第一次登陆的数据
        KeyedStream<JSONObject, String> keyedStream = jsonObjectDS
            .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // 状态编程, 核心
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            // 日期状态
            private ValueState<String> dateState;
            private SimpleDateFormat sdf ;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("date-state", String.class);

                // 这边设置状态过期时间, 没有必要设置定时器
                StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.hours(24))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
                valueStateDescriptor.enableTimeToLive(ttlConfig);

                    dateState = getRuntimeContext().getState(valueStateDescriptor);
                sdf = new SimpleDateFormat("yyyy-MM-dd");



            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                // 取出上一跳页面信息
                String lastPageId = value.getJSONObject("page").getString("last_page_id");

                // 判断上一跳页面是否为null
                if(lastPageId == null || lastPageId.length() <= 0){
                    // 取出状态数据
                    String lastDate = dateState.value();

                    // 取出今天的日期
                    String currentDate = sdf.format(value.getLong("ts"));

                    // 判断两个日期是否相同
                    if(!currentDate.equals(lastDate)){
                        // 如果不等, 更新当前的日期
                        dateState.update(currentDate);
                        return true;
                    }
                    //else{
                    //    // 如果相等, 今天来过了
                    //    return false;
                    //}
                }
                //else { // 上一跳不为空
                //    return false;
                //}
                return false;
            }
        });

        // TODO 5. 将数据写入kafka
        uvDS.print();

        // 每个mid, 每天只会发送一条数据进入 dwm_unique_visit 这个主题
        uvDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));


        // TODO 6. 启动任务
        env.execute();
    }
}
