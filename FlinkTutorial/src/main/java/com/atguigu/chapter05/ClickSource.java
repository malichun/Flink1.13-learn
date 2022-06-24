package com.atguigu.chapter05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * 自定义Source
 * @author malichun
 * @create 2022/6/19 23:47
 */
public class ClickSource implements SourceFunction<Event> {
    // 标识位, 用于控制数据的生成
    private boolean running = true;

    // 在指定的数据集中随机选取数据
    Random random = new Random();

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        // 循环, 不停的生成数据
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        while(running){
            ctx.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
            // 隔 1 秒生成一个点击事件，方便观测
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
