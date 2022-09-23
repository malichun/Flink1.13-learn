package com.atguigu.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * 1. 实现接口interceptor
 * 2. 实现抽象方法
 * 3. 静态内部类builder实现接口builder
 *
 * @author malichun
 * @create 2022/07/27 0027 23:32
 */
public class ETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        // 需求:过滤event中的数据是否是json格式
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);

        boolean flag = JSONUtils.isJSON(log);
        return flag ? event : null;
    }


    // 重要
    @Override
    public List<Event> intercept(List<Event> events) {
        // 将处理过之后为null的event删除掉
        // 使用迭代器
        Iterator<Event> iterator = events.iterator();

        // 可以使用lambda表达式替换
        //while(iterator.hasNext()){
        //    Event event = iterator.next();
        //    if(intercept(event) == null){
        //        iterator.remove();
        //    }
        //}

        events.removeIf(Objects::isNull);
        return events;
    }

    @Override
    public void close() {

    }


    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        // 读取配置文件
        @Override
        public void configure(Context context) {

        }
    }
}
