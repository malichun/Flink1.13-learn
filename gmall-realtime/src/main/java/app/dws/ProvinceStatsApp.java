package app.dws;

import bean.ProvinceStats;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import utils.ClickhouseUtil;
import utils.MyKafkaUtil;

/**
 * 地区主题宽表 使用FlinkSQL实现
 * 使用窗口表值函数
 *
 * 主要: 订单+订单明细
 *  数据流: WebApp -> nginx -> SpringBOot -> MySql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka/Hbase(dwd-dim) -> FlinkApp(redis) -> Kafka(dwm) -> FlinkApp -> Clickhouse
 *
 *  程序:     MockDb                     -> Mysql  -> FlinkCDC -> Kafka(zk) -> BaseDbApp -> Kafka/Phoenix(zk/hdfs/hbase) -> OrderWideApp(redis) -> Kafka -> ProvinceStatsApp -> Clickhouse
 *
 */
public class ProvinceStatsApp {
    public static void main(String[] args) throws Exception{
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        // TODO 2.使用DDL创建表, 提取时间戳生成watermark
        String orderWideTopic = "dwm_order_wide";
        String groupId = "province_stats";
        String ddl = "CREATE TABLE order_wide (\n" +
            "    province_id bigint,\n"+
            "    province_name string,\n" +
            "    province_area_code string,\n" +
            "    province_iso_code string,\n" +
            "    province_3166_2_code string,\n" +
            "    order_id    bigint,  \n" +
            "    split_total_amount decimal, \n" +
            "    create_time string, \n"+
            "    rt as TO_TIMESTAMP(`create_time`), \n" +
            "    WATERMARK FOR rt AS rt - INTERVAL '1' SECOND\n" +
            ")  WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")";
        System.out.println(ddl);
        tableEnv.executeSql(ddl);
        Table table1 = tableEnv.sqlQuery("select * from order_wide");
        tableEnv.toDataStream(table1).print("order_wide>>>>>>>>>>>>");

        System.out.println("========");
        // TODO 3.查询数据 分组,开窗,聚合
        // 使用表值窗口函数
        String sql = "select \n" +
            "    DATE_FORMAT(window_start,'yyyy-MM-dd HH:mm:ss') stt,\n" +
            "    DATE_FORMAT(window_end,'yyyy-MM-dd HH:mm:ss') edt, \n"+
            "    province_id,\n" +
            "    province_name,\n" +
            "    province_area_code,\n" +
            "    province_iso_code,\n" +
            "    province_3166_2_code,\n" +
            "    sum(split_total_amount) as order_amount, \n" + // 订单金额
            "    count(distinct order_id) as order_count, \n" + // 订单数
            "    UNIX_TIMESTAMP()*1000 ts\n" +
            "from \n" +
            "    TABLE(TUMBLE(TABLE order_wide, DESCRIPTOR(rt), INTERVAL '10' SECOND))\n" + // 使用窗口表值函数
            "group by \n" +
            "    province_id,\n" +
            "    province_name,\n" +
            "    province_area_code,\n" +
            "    province_iso_code,\n" +
            "    province_3166_2_code,\n" +
            "    window_start,\n" +
            "    window_end";
        System.out.println(sql);
        Table table = tableEnv.sqlQuery(sql);

        // TODO 4.将动态表转换为流, 使用AppendStream
        // FIXME 使用 toDataStream会有类型问题, 待解决(BigDecimal类型)
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(table, ProvinceStats.class);

        // TODO 5.打印数据并写入clickhouse
        provinceStatsDataStream.print();
        provinceStatsDataStream.addSink(ClickhouseUtil.getSink("insert into province_stats_2021 values(?,?,?,?,?,?,?,?,?,?)"));

        // TODO 6.启动任务
        env.execute();

    }
}
