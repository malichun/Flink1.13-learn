package utils;

import bean.TransientSink;
import common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

/**
 *
 * 反射: obj.getField()  => field.get(objt)
 *      obj.method(args)  => method.invoke(obj, args)
 * @author malichun
 * @create 2022/07/16 0016 13:55
 */
public class ClickhouseUtil {
    public static <T> SinkFunction<T> getSink(String sql) {
        return JdbcSink.<T>sink(
            sql,
            (JdbcStatementBuilder<T>) (preparedStatement, t) -> {
                try {
                    // 获取所有的属性信息
                    Field[] declaredFields = t.getClass().getDeclaredFields();
                    int skipOffset = 0; //
                    // 遍历字段
                    for (int i = 0; i < declaredFields.length; i++) {
                        // 获取字段
                        Field field = declaredFields[i];

                        // 设置私有属性可以访问
                        field.setAccessible(true);

                        // 获取字段上注解
                        TransientSink annotation = field.getAnnotation(TransientSink.class);
                        // 存在该注解
                        if(annotation!=null){
                            skipOffset++;
                            continue;
                        }


                        // 获取值
                        Object value = field.get(t);

                        // 给预编译SQL对象赋值
                        preparedStatement.setObject(i + 1 - skipOffset, value);
                    }

                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(5)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                .withUrl(GmallConfig.CLICKHOUSE_URL)
                .withUsername(GmallConfig.CLICKHOUSE_USERNAME)
                .withPassword(GmallConfig.CLICKHOUSE_PASSWORD)
                .build()
        );
    }
}
