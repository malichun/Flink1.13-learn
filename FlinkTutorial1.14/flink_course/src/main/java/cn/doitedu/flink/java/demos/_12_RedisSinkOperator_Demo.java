package cn.doitedu.flink.java.demos;

import com.alibaba.fastjson.JSON;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author malichun
 * @create 2022/08/28 0028 13:43
 */
public class _12_RedisSinkOperator_Demo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        // 构造好一个数据源
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        // eventLog 数据插入redis, 你想用什么结构来存储?
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("localhost").build();
        RedisSink<EventLog> redisSink = new RedisSink<>(config, new MyRedisInsertMapper());

        streamSource.addSink(redisSink);


        env.execute();
    }

    static class MyRedisInsertMapper implements RedisMapper<EventLog>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.RPUSH, "eventlogs");
        }

        /**
         * 这里是redis数据结构中的大key
         * @param data
         * @return
         */
        @Override
        public String getKeyFromData(EventLog data) {
            return "eventlogs";
        }

        @Override
        public String getValueFromData(EventLog data) {
            return JSON.toJSONString(data);
        }
    }
}
