package app.function;

import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static common.GmallConfig.*;

/**
 * 广播流数据处理
 *
 * @author malichun
 * @create 2022/07/10 0010 13:13
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private OutputTag<JSONObject> hbaseTag; // 侧输出流输出用
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor; // 广播流的描述器

    public TableProcessFunction(OutputTag<JSONObject> hbaseTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.hbaseTag = hbaseTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(PHOENIX_DRIVER);
        connection = DriverManager.getConnection(PHOENIX_SERVER);
    }

    /**
     * 1.获取并解析数据
     * 2. 建表
     * 3. 写入状态, 广播出去
     * <p>
     * 自定义反序列化器的格式
     * value:{"db":"", "tn":"", "before":{}, "after":{}, "type":""}
     */
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        // 1. 获取并解析数据
        JSONObject jsonObject = JSON.parseObject(value);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);


        // 2. 建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            // 建hbase表
            checkTable(tableProcess.getSinkTable(), // 表名
                tableProcess.getSinkColumns(), // 字段
                tableProcess.getSinkPk(), // 主键
                tableProcess.getSinkExtend() // 建表语句后面的部分
            );
        }

        // 3.写入状态, 广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);

    }

    /**
     * 建表语句
     * create table if not exists db.tn(
     * ID varchar primary key,
     * C1.STATUS varchar,
     * C1.MONEY float,
     * C1.PAY_WAY integer,
     * C1.USER_ID varchar,
     * C1.OPERATION_TIME varchar,
     * C1.CATEGORY varchar
     * );
     * <p>
     * <p>
     * mysql表结构:
     * source_table
     * operate_type
     * sink_type
     * sink_table
     * sink_columns
     * sink_pk
     * sink_extend
     *
     * @param sinkTable
     * @param sinkColumns
     * @param sinkPk
     * @param sinkExtend
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) throws SQLException {
        PreparedStatement preparedStatement = null;
        try {
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }
            StringBuilder createTableSQL = new StringBuilder("create table if not exists ")
                .append(HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");
            String[] fields = sinkColumns.split(",");
            for (int i = 0; i < fields.length; i++) {
                String field = fields[i];
                // 判断是否为主键
                if (sinkPk.equals(field)) {
                    createTableSQL.append(field)
                        .append(" varchar primary key ");
                } else {
                    createTableSQL.append(field).append(" varchar ");
                }
                // 判断是否为最后一个字段, 如果不是则添加 ","
                if (i < fields.length - 1) {
                    createTableSQL.append(" , ");
                }
            }
            createTableSQL.append(")")
                .append(sinkExtend);

            // 打印建表语句
            System.out.println(createTableSQL);

            // 预编译sql
            preparedStatement = connection.prepareStatement(createTableSQL.toString());
            // 执行
            preparedStatement.execute();
        } catch (SQLException e) {
            // 建表异常
            throw new RuntimeException("Phoenix表"+ sinkTable+" 建表失败!");
        }finally {
            if(preparedStatement!=null) {
                preparedStatement.close();
            }
        }
    }

    /**
     * 主流处理
     * 1. 获取状态数据
     * 2. 过滤字段
     * 3. 分流( hbase流和kafka流 )
     * <p>
     * 自定义反序列化器的格式
     * * value:{"database":"", "tableName":"", "before":{}, "after":{}, "type":""}
     */
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        // 1.获取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "-" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {

            // 2.过滤字段
            JSONObject data = value.getJSONObject("after");
            // 对after数据进行过滤剪裁
            filterColumn(data, tableProcess.getSinkColumns());

            // 3. 分流, kafka数据
            // 将输出表/主题信息写入value // 将不同类型的数据写入到kafka/hbase的kafka消息, 带上kafka的topic信息和 hbase表的信息, 下游根据这个发送到不同的kafka
            value.put("sinkTable", tableProcess.getSinkTable());
            if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                // kafka数据, 写入主流
                out.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                // hbase数据, 写入侧输出流
                ctx.output(hbaseTag, value);
            }
        } else {
            System.out.println("该组合key " + key + " 不存在!");
        }

    }

    /**
     * @param data        {"id":"11", "tm_name":"atguigu", "logo_url":"aaa:}
     * @param sinkColumns id,tm_name
     *                    // 经过处理后: {"id":"11", "tm_name":"atguigu"}
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fields);
        // 复杂写法
        //Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
        //while(iterator.hasNext()){
        //    Map.Entry<String, Object> next = iterator.next();
        //    if(!columns.contains(next.getKey()))
        //        iterator.remove();
        //}

        // 简写
        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));
    }


}
