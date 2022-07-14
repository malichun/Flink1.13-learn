package app.dwm;

import app.function.DimAsyncFunction;
import bean.OrderDetail;
import bean.OrderInfo;
import bean.OrderWide;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.MyKafkaUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * 第四章. 订单宽表
 *  数据流: WebApp -> nginx -> SpringBOot -> MySql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka/Hbase(dwd-dim) -> FlinkApp(redis) -> Kafka(dwm)
 *
 *  程序:     MockDb                     -> Mysql  -> FlinkCDC -> Kafka(zk) -> BaseDbApp -> Kafka/Phoenix(zk/hdfs/hbase) -> OrderWideApp(redis) -> Kafka
 *
 * @author malichun
 * @create 2022/07/12 0012 0:44
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1. 获取执行环境
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

        // TODO 2.读取Kafka 主题数据, 并转换为JavaBean对象& 提取时间戳生成WaterMark
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide"; // 输出kafka
        String groupId = "order_wide_group";

        SingleOutputStreamOperator<OrderInfo> orderInfoDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId))
            .map(line -> {
                OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);
                String create_time = orderInfo.getCreate_time();
                String[] dateTimeArr = create_time.split(" ");
                orderInfo.setCreate_date(dateTimeArr[0]);
                orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                orderInfo.setCreate_ts(sdf.parse(create_time).getTime());
                return orderInfo;
            }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId))
            .map(line -> {
                OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
                String create_time = orderDetail.getCreate_time();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                orderDetail.setCreate_ts(sdf.parse(create_time).getTime());
                return orderDetail;
            }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));


        // TODO 3.双流join
        // 没有维度数据
        SingleOutputStreamOperator<OrderWide> orderWideWithNoDimDS = orderInfoDS.keyBy(OrderInfo::getId)
            .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
            .between(Time.seconds(-5), Time.seconds(5)) // 生产环境中给最大的延迟时间
            .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                @Override
                public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context ctx, Collector<OrderWide> out) throws Exception {
                    out.collect(new OrderWide(orderInfo, orderDetail));
                }
            });

        //打印测试
        //orderWideWithNoDimDS.print("orderWideWithNoDimDS ");

        // TODO 4.关联维度信息 Hbase Phoenix, 采用异步
        //orderWideWithNoDimDS.map(orderWide -> {
        //    // 关联用户维度
        //    Long user_id = orderWide.getUser_id();
        //
        //    // 根据user_id查询phoenix用户信息
        //
        //    // 将用户信息补充至orderWide
        //
        //    // 返回结果
        //    return orderWide;
        //});

        // 4.1 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
            orderWideWithNoDimDS,
            new DimAsyncFunction<OrderWide>("DIM_USER_INFO"){
                @Override
                public String getKey(OrderWide input) {
                    return input.getUser_id().toString();
                }

                @Override
                public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                    orderWide.setUser_gender(dimInfo.getString("GENDER"));
                    String birthday = dimInfo.getString("BIRTHDAY");
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    long currentTs = System.currentTimeMillis();
                    long ts = sdf.parse(birthday).getTime();
                    long age = (currentTs - ts) / (1000 * 60 * 60 * 24 * 365L);
                    orderWide.setUser_age((int)age);
                }
            },
            60,
            TimeUnit.SECONDS
        );

        // 4.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {

            @Override
            public String getKey(OrderWide input) {
                return input.getProvince_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                orderWide.setProvince_name(dimInfo.getString("NAME"));
                orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
            }
        }, 10, TimeUnit.SECONDS);

        // 4.3 关联sku维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);

        // 4.4 关联spu维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);


        // 4.5关联TM维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
            orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        // 4.6 关联category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        // 打印测试
        orderWideWithCategory3DS.print("orderWideWithUserDS");


        // TODO 5.将数据写入kafka
        orderWideWithCategory3DS
            .map(JSONObject::toJSONString)
            .addSink(MyKafkaUtil.getKafkaProducer(orderWideSinkTopic));

        // TODO 6. 启动任务
        env.execute("OrderWideApp");
    }
}
