package app.dws;

import bean.VisitorStats;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.ClickhouseUtil;
import utils.DateTimeUtil;
import utils.MyKafkaUtil;

import java.time.Duration;
import java.util.Date;

/**
 *
 * Desc: DWS层访客宽表计算
 * <p>
 * ?要不要把多个明细的同样的维度统计在一起?
 * 因为单位时间内 mid 的操作数据非常有限不能明显的压缩数据量（如果是数据量够大，或者单位时间够长可以）
 * 所以用常用统计的四个维度进行聚合 渠道、新老用户、app 版本、省市区域
 * 度量值包括 启动、日活（当日首次启动）、访问页面数、新增用户数、跳出数、平均页面停留时长、总访问时长
 * 聚合窗口： 10 秒
 * <p>
 * 各个数据在维度聚合前不具备关联性，所以先进行维度聚合
 * 进行关联 这是一个 fulljoin
 * 可以考虑使用 flinksql 完成
 *
 * 分流
 * 数据流: web/app -> Nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> Kafka(dwm)  -> FlinkApp -> CLickHouse
 *
 * 程 序: mockLog -> Nginx -> Logger.sh -> kafka(ZK) -> BaseLogApp -> Kafka -> uv/uj -> Kafka -> VisitorStatsApp -> ClickHouse
 *
 *
 */
public class VisitorStartsApp {
    public static void main(String[] args) throws Exception{
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

        //TODO 2.从 Kafka 的 pv、uv、跳转明细主题中获取数据
        String groupId = "visitor_stat_app";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String pageViewSourceTopic = "dwd_page_log";

        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId));
        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));

        // TODO 3. 将每个流处理成相同的数据类型
        // 3.1处理UV数据
        SingleOutputStreamOperator<VisitorStats> visitorStatWithUvDS = uvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            // 提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                common.getString("vc"),
                common.getString("ch"),
                common.getString("ar"),
                common.getString("is_new"),
                1L,
                0L,
                0L,
                0L,
                0L,
                jsonObject.getLong("ts")
            );
        });

        // 3.2处理UJ数据, FIXME: 计算完,窗口早关了
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUjDS = ujDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            // 提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                common.getString("vc"),
                common.getString("ch"),
                common.getString("ar"),
                common.getString("is_new"),
                0L,
                0L,
                0L,
                1L,
                0L,
                jsonObject.getLong("ts")
            );
        });

        // 3.3 处理PV数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPvDS = pvDS.map(line -> {
            // 获取公共字段
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            // 获取上一跳页面id
            JSONObject page = jsonObject.getJSONObject("page");
            String lastPageId = page.getString("last_page_id");

            // 求进入页面数
            long sv = 0L;
            if (lastPageId == null || lastPageId.length() == 0) {
                sv = 1L;
            }
            return new VisitorStats("", "",
                common.getString("vc"),
                common.getString("ch"),
                common.getString("ar"),
                common.getString("is_new"),
                0L,
                1L,
                sv, // 进入页面数
                0L,
                page.getLong("during_time"),
                jsonObject.getLong("ts")
            );
        });

        // TODO 4.Union几个流
        DataStream<VisitorStats> unionDS = visitorStatWithUvDS
            .union(visitorStatsWithUjDS)
            .union(visitorStatsWithPvDS);

        // TODO 5.提取时间戳生成watermark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11)) // 考虑到跳出率使用CEP,
            .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                @Override
                public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                    return element.getTs();
                }
            }));

        // TODO 6.按照维度信息分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWMDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                // 获取维度信息
                return Tuple4.of(value.getAr(), value.getCh(), value.getIs_new(), value.getVc());
            }
        });

        // TODO 7.开窗聚合 10s的滚动窗口
        // 滚动窗口
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream
            .window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<VisitorStats> result = windowedStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                // FixMe 如果是滑动窗口, 只能new, 不能修改value1
                return new VisitorStats(value1.getStt(), value1.getEdt(), value1.getVc(), value1.getCh(), value1.getAr(), value1.getIs_new(),
                    value1.getUv_ct() + value2.getUv_ct(), value1.getPv_ct() + value2.getPv_ct(), value1.getSv_ct() + value2.getSv_ct(),
                    value1.getUj_ct() + value2.getUj_ct(), value1.getDur_sum() + value2.getDur_sum(), value1.getTs()
                );
            }
        }, new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() { //<IN, OUT, KEY, W>

            @Override
            public void process(Tuple4<String, String, String, String> key,
                                ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>.Context context,
                                Iterable<VisitorStats> elements,
                                Collector<VisitorStats> out) throws Exception {
                TimeWindow window = context.window();
                long start = window.getStart();
                long end = window.getEnd();
                VisitorStats visitorStats = elements.iterator().next();
                // 补充窗口信息
                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)));
                out.collect(visitorStats);

            }
        });

        // TODO 8. 将数据写入clickhouse
        result.print(">>>>>>>>>>");
        //result.addSink(JdbcSink.sink())
        result.addSink(ClickhouseUtil.getSink("insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        // TODO 9.任务的启动
        env.execute("VisitorStartsApp");

    }
}
