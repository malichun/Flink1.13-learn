package common;

/**
 * 常量类
 * @author malichun
 * @create 2022/07/10 0010 13:18
 */
public class GmallConfig {
    //Phoenix 库名
    public static final String HBASE_SCHEMA = "GMALL2021_REALTIME";
    //Phoenix 驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    //Phoenix 连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    // ClickHouse_Url
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/default";
    public static final String CLICKHOUSE_USERNAME = "default";
    public static final String CLICKHOUSE_PASSWORD = "123456";
}
