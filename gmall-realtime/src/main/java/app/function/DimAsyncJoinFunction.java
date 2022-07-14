package app.function;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @author malichun
 * @create 2022/07/14 0014 0:46
 */
public interface DimAsyncJoinFunction<T> {

    String getKey(T input);

    void join(T input, JSONObject dimInfo) throws ParseException;
}
