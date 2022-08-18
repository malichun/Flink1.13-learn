package cn.doitedu.flink.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 时态join代码示例
 * 版本表, 动态带主键的表, 做维表
 *
 * @author malichun
 * @create 2022/08/18 0018 0:30
 */
public class Demo20_TemporalJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        Configuration configuration = tenv.getConfig().getConfiguration();

        // 设置table环境中的状态ttl时长
        configuration.setLong("table.exec.state.ttl", 60 * 60 * 1000); // 设置状态过期时间

        /**
         *
         * 主表
         *订单Id, 币种, 金额, 订单时间
         * 1,a,100,167438436400
         */
        SingleOutputStreamOperator<Order> orderDs = env.socketTextStream("hadoop102", 6666)
            .map(s -> {
                String[] arr = s.split(",");
                return new Order(Integer.parseInt(arr[0]), arr[1], Double.parseDouble(arr[2]), Long.parseLong(arr[3]));
            }).returns(Order.class);

        tenv.createTemporaryView("orders", orderDs, Schema.newBuilder()
            .column("orderId", DataTypes.INT())
            .column("currency", DataTypes.STRING())
            .column("price", DataTypes.DOUBLE())
            .column("orderTime", DataTypes.BIGINT())
            .columnByExpression("rt", "to_timestamp_ltz(orderTime,3)")
            .watermark("rt", "rt")
            .build());

        // 创建 temporal 表, 要带主键,时间属性, 读取变化流nc
        tenv.executeSql("CREATE TABLE currency_rate (\n" +
            "    currency string,\n" +
            "    rate double,\n" +
            //"    `rt` TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,\n" + // 要有一个字段表示时间, 可以从元数据里面获取
            "    update_time bigint,\n"+
            "    rt as to_timestamp_ltz(update_time,3),\n"+
            "    watermark for rt as rt - interval '0' second,\n"+
            "    primary key(currency) not enforced\n" +
            ") WITH (\n" +
            "    'connector' = 'mysql-cdc',\n" +
            "    'hostname' = 'hadoop102',\n" +
            "    'port' = '3306',\n" +
            "    'username' = 'root',\n" +
            "    'password' = '123456',\n" +
            "    'database-name' = 'flinktest',\n" +
            "    'table-name' = 'currency_rate'\n" +
            ")");


        // 关联查询
        tenv.executeSql("SELECT\n" +
            "    a.orderId,\n" +
            "    a.currency,\n" +
            "    a.price,\n" +
            "    a.orderTime,\n" +
            "    currency_rate.rate\n" +
            "from  orders a\n" +
            "left join currency_rate  for system_time as of a.rt\n" +
            "on a.currency = currency_rate.currency").print();

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        public int orderId;
        public String currency; // 货币
        public double price; //价格
        public Long orderTime; // 订单时间
    }
}
