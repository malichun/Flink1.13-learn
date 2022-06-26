package com.atguigu.chapter05_StreamAPI;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author malichun
 * @create 2022/6/24 0:20
 */
public class Sink03ToRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 创建一个jedis连接配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("192.168.1.12")
            .build();

        // 数据写入redis中
        stream.addSink(new RedisSink<>(config, new MyRedisMapper()));

        env.execute();
    }

    // 自定义类实现RedisMapper接口
    public static class MyRedisMapper implements RedisMapper<Event>{

        // 返回redis操作的命令描述
        @Override
        public RedisCommandDescription getCommandDescription() {
            // hash表导入
            return new RedisCommandDescription(RedisCommand.HSET, "clicks");
        }

        @Override
        public String getKeyFromData(Event data) {
            return data.user;
        }

        @Override
        public String getValueFromData(Event data) {
            return data.url;
        }
    }
}
