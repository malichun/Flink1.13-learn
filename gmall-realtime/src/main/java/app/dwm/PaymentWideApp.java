package app.dwm;

import bean.OrderWide;
import bean.PaymentInfo;
import bean.PaymentWide;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.DateTimeUtil;
import utils.MyKafkaUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 第5章 支付宽表
 *  数据流: WebApp -> nginx -> SpringBOot -> MySql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka/Hbase(dwd-dim) -> FlinkApp(redis) -> Kafka(dwm) -> FlinkApp -> Kafka(dwm)
 *
 *  程序:     MockDb                     -> Mysql  -> FlinkCDC -> Kafka(zk) -> BaseDbApp -> Kafka/Phoenix(zk/hdfs/hbase) -> OrderWideApp(redis) -> Kafka -> PaymentWideApp -> Kafka
 *
 * @author malichun
 * @create 2022/07/12 0012 0:44
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
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

        // TODO 2.读取Kafka主题的数据,创建流, 并转换为JavaBean对象, 提取时间戳生成watermark
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        // 订单宽表流
        SingleOutputStreamOperator<OrderWide> orderWideDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId))
            .map(line -> JSON.parseObject(line, OrderWide.class))
            .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps() // 反正要给时间范围, 就让watermark = 0
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide element, long recordTimestamp) {
                        //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        //try {
                        //    return sdf.parse(element.getCreate_time()).getTime();
                        //} catch (ParseException e) {
                        //    e.printStackTrace();
                        //    return recordTimestamp; // 进入系统的时间
                        //}
                        // 使用时间工具类
                        return DateTimeUtil.toTs(element.getCreate_time());
                    }
                })
            );
        // 支付宽表流
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId))
            .map(line -> JSON.parseObject(line, PaymentInfo.class))
            .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                        //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        //try {
                        //    return sdf.parse(element.getCreate_time()).getTime();
                        //} catch (ParseException e) {
                        //    e.printStackTrace();
                        //    return recordTimestamp; // 进入系统的时间
                        //}
                        return DateTimeUtil.toTs(element.getCreate_time());
                    }
                }));

        // TODO 3. 双流join
        // 支付订单在前
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
            .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
            .between(Time.minutes(-15), Time.seconds(5))
            .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {

                @Override
                public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context ctx, Collector<PaymentWide> out) throws Exception {
                    out.collect(new PaymentWide(paymentInfo, orderWide));
                }
            });

        // TODO 4. 将数据写入kafka
        paymentWideDS.print("paymentWideDS>>>>");

        paymentWideDS
            .map(JSONObject::toJSONString)
            .addSink(MyKafkaUtil.getKafkaProducer(paymentWideSinkTopic));

        // TODO 5.启动任务
        env.execute();

    }
}
