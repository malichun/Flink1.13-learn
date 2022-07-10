package app.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @author malichun
 * @create 2022/07/09 0009 22:31
 */
public class CustomerDebeziumDeserialization implements DebeziumDeserializationSchema<String> {


    /**
     * 封装的数据格式
     * {
     *     "database":"",
     *     "tableName":"",
     *     "before"{
     *         "":"","":""....
     *     },
     *     "after":{
     *        "":"","":""....
     *     },
     *     "type":"c u d",
     *     //"ts":1564...
     * }
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // 1. 创建json对象,用于存储最终数据
        JSONObject result = new JSONObject();

        // 2.获取库名和表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];

        Struct value = (Struct)sourceRecord.value();
        // 3. 获取before数据
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if(before != null ){
            Schema beforeSchema = before.schema();
            for (Field field : beforeSchema.fields()) {
                Object beforeValue = before.get(field);
                beforeJson.put(field.name(), beforeValue);
            }
        }

        // 4.获取after数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if(after != null ){
            Schema afterSchema = after.schema();
            for (Field field : afterSchema.fields()) {
                Object afterValue = after.get(field);
                afterJson.put(field.name(), afterValue);
            }
        }
        // 5.获取操作类型, ,CREATE , UPDATE, DELETE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if("create".equals(type)){
            type = "insert";
        }

        // 6. 将字段写入JSON对象
        result.put("database",database);
        result.put("tableName", tableName);
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("type", type);

        // 7.输出数据
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
