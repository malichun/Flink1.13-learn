package app.dws;

import app.function.SplitFunction;
import bean.KeywordStats;
import common.GmallConstant;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import scala.tools.nsc.doc.html.Page;
import utils.ClickhouseUtil;
import utils.MyKafkaUtil;

/**
 * @author malichun
 * @create 2022/07/18 0018 23:49
 */
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception{
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.使用DDL方式读取Kafka数据
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String ddl =
            "  create table page_view(\n" +
            "    common map<string,string>,\n" +
            "    page map<String,String>,\n" +
            "    ts bigint,\n" +
            "    rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')),\n" +
            "    watermark for rowtime as rowtime - interval '2' second\n" +
            ") with ("+ MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")";
        System.out.println(ddl);
        tableEnv.executeSql(ddl);

        // TODO 3.过滤数据 上一跳页面为search and 搜索词 is not null
        Table  fullwordView =  tableEnv.sqlQuery("SELECT\n" +
            "    page['item'] fullword,\n" +
            "    rowtime\n" +
            "from \n" +
            "    page_view\n" +
            "where \n" +
            "    page['page_id']='good_list'\n" +
            "    and page['item'] is not null");


        // TODO 4.注册UDTF, 使用udtf函数进行分词处理
        tableEnv.createTemporarySystemFunction("split_function", SplitFunction.class);
        Table keywordView = tableEnv.sqlQuery("select\n" +
            "    keyword,\n" +
            "    rowtime\n" +
            "from  \n" +
            fullwordView  +" , lateral table(split_function(fullword)) as T(keyword)");
        tableEnv.toDataStream(keywordView).print("keywordView>>>");
        tableEnv.createTemporaryView("keywordView", keywordView);

        // TODO 5. 分组,开窗,聚合
        String resSql = "select\n" +
            "    keyword,\n" +
            "    DATE_FORMAT(window_start,'yyyy-MM-dd HH:mm:ss') as stt,\n" +
            "    DATE_FORMAT(window_end,'yyyy-MM-dd HH:mm:ss') as edt,\n" +
            "   '" + GmallConstant.KEYWORD_SEARCH +"' as source ,\n"+
            "    UNIX_TIMESTAMP()*1000 as ts, \n"+
            "    count(1) as ct\n" +
            "from \n" +
            "    TABLE(TUMBLE(TABLE keywordView, DESCRIPTOR(rowtime), INTERVAL '10' SECONDS))\n" +
            "group by \n" +
            "    keyword,\n" +
            "    window_start,\n" +
            "    window_end";

        System.out.println("=============\n"+resSql);

        Table keywordStatsSearch = tableEnv.sqlQuery(resSql);

        tableEnv.toDataStream(keywordStatsSearch).print("rrrrr");

        // TODO 6.将动态表转化为流
        DataStream<KeywordStats> keywordStatsSearchDataStream = tableEnv.toAppendStream(keywordStatsSearch, KeywordStats.class);
        // TODO 7.将数据打印写入clickhouse
        keywordStatsSearchDataStream.print("result: ");

        keywordStatsSearchDataStream.addSink(ClickhouseUtil.getSink("insert into keyword_stats(keyword,ct,source,stt,edt,ts)" +
            "values(?,?,?,?,?,?)"));
        // TODO 8.启动任务
        env.execute("KeywordStatsApp");
    }
}
