package app.function;

import com.alibaba.fastjson.JSONObject;
import common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * phoenix 自定义sink
 * @author malichun
 * @create 2022/07/10 0010 17:58
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        // phoenix需要设置自动提交
        connection.setAutoCommit(true);
    }

    //

    /**
     * value: {"sinkTable":"dim_base_trademark",
     *     "database":"gmall2021",
     *     "before":{"tm_name":"bbbb","logo_url":"/bbb","id":14},
     *     "after":{"tm_name":"Atguigu","id":14},
     *     "type":"update",
     *     "tableName":"base_trademark"}
     *
     * SQL: upsert into GMALL2021_REALTIME.dim_base_trademark(id,tm_name) values('14','Atguigu')
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        // 2.预编译sql
        PreparedStatement preparedStatement = null;
        try {
            // 1. 获取sql语句
            String upsertSql = genUpsertSql(
                value.getString("sinkTable"), // phoenix表名
                value.getJSONObject("after") // 字段
            );
            System.out.println(upsertSql);
            preparedStatement = connection.prepareStatement(upsertSql);

            // 3.执行插入操作
            preparedStatement.executeUpdate();

            // 手动提交
            //connection.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(preparedStatement!=null){
                preparedStatement.close();
            }
        }

    }

    // data: {"tm_name":"Atguigu","id":14}
    //SQL: upsert into GMALL2021_REALTIME.dim_base_trademark(id,tm_name) values('14','Atguigu')
    private String genUpsertSql(String sinkTable, JSONObject data) {
        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
            org.apache.commons.lang3.StringUtils.join(keySet, ",") + ") values('" +
            StringUtils.join(values, "','") + "')";

    }
}
