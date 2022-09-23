package cn.doitedu.flink.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 测试 TVF 窗口表值函数
 * 参考链接: https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/sql/queries/window-agg/
 *
 *
 * 示例:
 * +------------------+-------+------+-------------+
 * ,bidtime,price,item,supplier_id,
 * ------------------+-------+------+-------------+
 * 2020-04-15 08:05:00.000,4.00,C,supplier1
 * 2020-04-15 08:07:00.000,2.00,A,supplier1
 * 2020-04-15 08:09:00.000,5.00,D,supplier2
 * 2020-04-15 08:11:00.000,3.00,B,supplier2
 * 2020-04-15 08:13:00.000,1.00,E,supplier1
 * 2020-04-15 08:17:00.000,6.00,F,supplier2
 * ------------------+-------+------+-------------+
 * @author malichun
 * @create 2022/08/14 0014 21:37
 */
public class Demo16_TimeWindowDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // bidtime | price | item | supplier_id
        SingleOutputStreamOperator<Bid> s1 = env.socketTextStream("hadoop102", 8888)
            .map(s -> {
                String[] split = s.split(",");
                return new Bid(split[0], Double.parseDouble(split[1]), split[2], split[3]);
            });

        tenv.createTemporaryView("t_bid", s1, Schema.newBuilder()
            .column("bidtime", DataTypes.STRING()) //2020-04-15 08:05:00.000的字符串,
            .column("price", DataTypes.DOUBLE())
            .column("item", DataTypes.STRING())
            .column("supplier_id", DataTypes.STRING())
            .columnByExpression("rt", $("bidtime").toTimestamp())
            .watermark("rt", "rt - interval '1' second")
            .build());

        // 查询
        //tenv.executeSql("select bidtime, price, item, supplier_id,current_watermark(rt) as wm from t_bid").print();

        // 每分钟计算最近5分钟的交易总额
        tenv.executeSql("select\n" +
            "    window_start,\n" +
            "    window_end,\n" +
            "    sum(price) as price_amt\n" +
            "from \n" +
            "    table(hop(table t_bid, descriptor(rt), interval '1' minutes, interval '5' minutes))\n" +
            "group by \n" +
            "    window_start, window_end")
        /* .print() */;

        // 没2分钟计算最近2分钟的交易总额
        tenv.executeSql("select\n" +
            "    window_start,\n" +
            "    window_end,\n" +
            "    sum(price) as price_amt\n" +
            "from \n" +
            "    table(tumble(table t_bid, descriptor(rt), interval '2' minutes))\n" +
            "group by \n" +
            "    window_start, window_end")
        /* .print() */;

        // 每2分钟计算今天以来总的交易额
        tenv.executeSql("select\n" +
            "    window_start,\n" +
            "    window_end,\n" +
            "    sum(price) as price_amt\n" +
            "from \n" +
            "    table(CUMULATE(table t_bid, descriptor(rt), interval '2' minutes, interval '24' hour))\n" +
            "group by \n" +
            "    window_start, window_end"
        )/* .print() */;


        // 每10分钟计算一次最近10分钟内交易总额最大的前3个供应商及其交易单数
        tenv.executeSql("select\n" +
            "    window_start,\n" +
            "    window_end, \n" +
            "    supplier_id,\n" +
            "    price_amt,\n" +
            "    bit_cnt\n" +
            "from \n" +
            "(\n" +
            "    select\n" +
            "        window_start,\n" +
            "        window_end,\n" +
            "        supplier_id,\n" +
            "        price_amt,\n" +
            "        bit_cnt,\n" +
            "        row_number() over(partition by window_start, window_end order by price_amt desc) as rn\n" +
            "    from \n" +
            "    (\n" +
            "        SELECT\n" +
            "            window_start, \n" +
            "            window_end, \n" +
            "            supplier_id,\n" +
            "            sum(price) as price_amt,\n" +
            "            count(1) as bit_cnt\n" +
            "        from \n" +
            "            table(\n" +
            "                tumble(table t_bid, descriptor(rt), interval '10' minutes)\n" +
            "            )\n" +
            "        group by \n" +
            "            window_start, window_end, supplier_id\n" +
            "    ) t\n" +
            ") t where rn <= 3")/* .print() */;

        // 在滚动窗口上, 直接求TOPN
        tenv.executeSql("SELECT\n" +
            "    *\n" +
            "from \n" +
            "(\n" +
            "select\n" +
            "    bidtime,\n" +
            "    price,\n" +
            "    item,\n" +
            "    supplier_id,\n" +
            "    row_number() over(partition by window_start, window_end order by price desc ) as rn\n" +
            "from \n" +
            "    table(tumble(table t_bid,descriptor(rt),interval '10' minute))\n" +
            ") \n" +
            "where rn <= 2")
            .print();
        ;

    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bid {
        private String bidtime; // 2020-04-15 08:05:00
        private double price;
        private String item;
        private String supplier_id;
    }
}
