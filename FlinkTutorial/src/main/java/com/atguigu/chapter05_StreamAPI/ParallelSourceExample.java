package com.atguigu.chapter05_StreamAPI;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 *
 * 生成并行的数据流
 * @author malichun
 * @create 2022/6/19 23:53
 */
public class ParallelSourceExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new CustomSource()).setParallelism(2).print();
        env.execute();
    }

    public static class CustomSource implements ParallelSourceFunction<Event>{
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
}
