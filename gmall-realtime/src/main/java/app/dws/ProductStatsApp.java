package app.dws;

import app.function.DimAsyncFunction;
import bean.OrderWide;
import bean.PaymentWide;
import bean.ProductStats;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import common.GmallConstant;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.ClickhouseUtil;
import utils.DateTimeUtil;
import utils.MyKafkaUtil;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * 商品主题宽表
 *
 * 数据流: app/web -> nginx -> springboot -> kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> Clickhouse
 *        app/web -> nginx -> springboot -> mysql  -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka/Phoenix -> FlinkApp -> Kafka(dwm) -> FlinkApp -> Clickhouse
 *
 * 程 序 mock -> nginx -> logger.sh -> kafka(zk)/Phoenix/HDFS/HBase/zk -> Redis -> Clickhouse
 */
public class ProductStatsApp {
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

        // TODO 2.读取Kafka 7个主题数据, 创建数据流
        String groupId = "product_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";  // dwm
        String paymentWideSourceTopic = "dwm_payment_wide"; // dwm
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        // 页面 点击,曝光
        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));
        // 收藏
        DataStreamSource<String> favorDS = env.addSource(MyKafkaUtil.getKafkaConsumer(favorInfoSourceTopic, groupId));
        // 购物车
        DataStreamSource<String> cartDS = env.addSource(MyKafkaUtil.getKafkaConsumer(cartInfoSourceTopic, groupId));
        // 订单
        DataStreamSource<String> orderDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId));
        // 订单支付
        DataStreamSource<String> payDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentWideSourceTopic, groupId));
        // 退单
        DataStreamSource<String> refundDS = env.addSource(MyKafkaUtil.getKafkaConsumer(refundInfoSourceTopic, groupId));
        // 评价
        DataStreamSource<String> commentDS = env.addSource(MyKafkaUtil.getKafkaConsumer(commentInfoSourceTopic, groupId));

        // TODO 3.将7个流统一数据格式 (ProductStats)

        // 3.1 点击和曝光
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pvDS.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {
                // 将数据转换为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);
                // 取出配置信息
                JSONObject page = jsonObject.getJSONObject("page");
                String pageId = page.getString("page_id");

                Long ts = jsonObject.getLong("ts");
                // 是一个点击数据
                if ("good_detail".equals(pageId) && "sku_id".equals(page.getString("item_type"))) {
                    out.collect(ProductStats.builder()
                        .sku_id(page.getLong("item"))
                        .click_ct(1L)
                        .ts(ts)
                        .build());
                }
                // 尝试取出曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        // 判断是否是商品
                        if ("sku_id".equals(display.getString("item_type"))) {
                            out.collect(ProductStats.builder()
                                .sku_id(display.getLong("item"))
                                .display_ct(1L)
                                .ts(ts)
                                .build());
                        }
                    }
                }
            }
        });

        // 3.2收藏数据
        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            return ProductStats.builder()
                .sku_id(jsonObject.getLong("sku_id"))
                .favor_ct(1L)
                .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                .build();
        });

        // 3.3加入购物车
        SingleOutputStreamOperator<ProductStats> produceStatsWithCartDS = cartDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            return ProductStats.builder()
                .sku_id(jsonObject.getLong("sku_id"))
                .cart_ct(1L) // 添加购物车的数目
                .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                .build();
        });

        // 3.4订单数据流, 下单商品个数, 下单商品金额, 订单数(通过辅助字段)
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderDS.map(line -> {
            OrderWide orderWide = JSON.parseObject(line, OrderWide.class);
            HashSet<Long> orderIds = new HashSet<Long>();
            orderIds.add(orderWide.getOrder_id());
            return ProductStats.builder()
                .sku_id(orderWide.getSku_id())
                .order_sku_num(orderWide.getSku_num())
                .order_amount(orderWide.getOrder_price()) // 金额(订单和订单明细关联取明细的金额)
                .orderIdSet(orderIds) // 这边通过辅助字段求订单数!!!!!!!!!!!!!!!!!!
                .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                .build();
        });

        // 3.5支付 支付订单数 支付金额
        SingleOutputStreamOperator<ProductStats> produceStatsWithPaymentDS = payDS.map(line -> {
            PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(paymentWide.getOrder_id());
            return ProductStats.builder()
                .sku_id(paymentWide.getSku_id())
                .payment_amount(paymentWide.getOrder_price())
                .paidOrderIdSet(orderIds) // 支付的订单数量, 订单个数
                .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                .build();
        });

        // 3.6退款  退款订单数  退款金额
        SingleOutputStreamOperator<ProductStats> produceStatsWithRefundDS = refundDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("order_id"));
            return ProductStats.builder()
                .sku_id(jsonObject.getLong("sku_id"))
                .refundOrderIdSet(orderIds) // 退款订单id集合,
                .refund_amount(jsonObject.getBigDecimal("refund_amount")) // 退款金额
                .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                .build();
        });

        // 3.7 评价数据 评论订单数, 好评订单数
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            String appraise = jsonObject.getString("appraise"); // 评价字段
            long goodCt = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                goodCt = 1L;
            }
            return ProductStats.builder()
                .sku_id(jsonObject.getLong("sku_id"))
                .comment_ct(1L) // 数量
                .good_comment_ct(goodCt) // 好评数
                .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                .build();
        });

        // TODO 4.Union 7个流
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS // 点击,曝光
            .union(productStatsWithFavorDS, //  收藏
                produceStatsWithCartDS, //加入购物车
                productStatsWithOrderDS, // 下单
                produceStatsWithPaymentDS, // 支付
                produceStatsWithRefundDS, // 退款
                productStatsWithCommentDS // 评价
            );

        // TODO 5.提取时间戳, 生成watermark
        SingleOutputStreamOperator<ProductStats> productStatsWithWMDS = unionDS
            .assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                })
            );
        // TODO 6, 分组,开窗,聚合 按照sku_id分组, 10秒的滚动窗口,结合增量聚合(累加值), 全量聚合(提取窗口信息)
        SingleOutputStreamOperator<ProductStats> reduceDS = productStatsWithWMDS.keyBy(ProductStats::getSku_id)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .reduce(new ReduceFunction<ProductStats>() {
                @Override
                public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                    // 1. 点击/曝光
                    stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                    stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());

                    // 2.收藏
                    stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());

                    // 3.加入购物车
                    stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                    // 4.下单
                    stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                    stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                    //stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                    stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());

                    // 5.支付
                    stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));
                    stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                    //stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                    // 6.退款
                    stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                    //stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                    stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                    // 7.评价
                    stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                    stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                    return stats1;
                }
            }, new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() { //<IN, OUT, KEY, W>
                @Override
                public void process(Long aLong, ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>.Context context, Iterable<ProductStats> elements, Collector<ProductStats> out) throws Exception {
                    // 取出数据
                    ProductStats productStats = elements.iterator().next();

                    // 补充字段
                    // 设置窗口时间
                    productStats.setStt(DateTimeUtil.toYMDhms(new Date(context.window().getStart())));
                    productStats.setEdt(DateTimeUtil.toYMDhms(new Date(context.window().getEnd())));
                    // 设置订单数量
                    productStats.setOrder_ct((long) productStats.getOrderIdSet().size());
                    productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());
                    productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());

                    // 将数据写出
                    out.collect(productStats);
                }
            });

        // TODO 7.关联维度信息
        // 1. 关联
        // 7.1 关联SKU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSKUDS = AsyncDataStream.unorderedWait(
            reduceDS,
            new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                @Override
                public String getKey(ProductStats productStats) {
                    return productStats.getSku_id().toString();
                }

                @Override
                public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                    productStats.setSku_name(dimInfo.getString("SKU_NAME"));
                    productStats.setSku_price(dimInfo.getBigDecimal("PRICE"));
                    productStats.setSpu_id(dimInfo.getLong("SPU_ID"));
                    productStats.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                    productStats.setTm_id(dimInfo.getLong("TM_ID"));
                }
            }, 60, TimeUnit.SECONDS);

        // 7.2 关联SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(
            productStatsWithSKUDS,
            new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                @Override
                public void join(ProductStats productStats, JSONObject jsonObject) {
                    productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                }

                @Override
                public String getKey(ProductStats productStats) {
                    return String.valueOf(productStats.getSpu_id());
                }
            }, 60, TimeUnit.SECONDS);

        // 7.3关联 category维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS = AsyncDataStream.unorderedWait(productStatsWithSpuDS,
            new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                @Override
                public void join(ProductStats productStats, JSONObject jsonObject) {
                    productStats.setCategory3_name(jsonObject.getString("NAME"));
                }

                @Override
                public String getKey(ProductStats productStats) {
                    return String.valueOf(productStats.getCategory3_id());
                }
            }, 60, TimeUnit.SECONDS);

        // 7.4 关联TM维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS = AsyncDataStream.unorderedWait(productStatsWithCategory3DS,
            new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                @Override
                public void join(ProductStats productStats, JSONObject jsonObject) {
                    productStats.setTm_name(jsonObject.getString("TM_NAME"));
                }

                @Override
                public String getKey(ProductStats productStats) {
                    return String.valueOf(productStats.getTm_id());
                }
            }, 60, TimeUnit.SECONDS);

        // TODO 8.将数据写入clickhouse
        productStatsWithTmDS.print("result>>>>>");
        productStatsWithTmDS.addSink(ClickhouseUtil.getSink("insert into product_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        // TODO 9.启动任务
        env.execute();
    }
}
