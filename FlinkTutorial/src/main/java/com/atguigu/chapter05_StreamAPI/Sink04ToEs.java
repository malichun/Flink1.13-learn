package com.atguigu.chapter05_StreamAPI;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author malichun
 * @create 2022/6/24 21:08
 */
public class Sink04ToEs {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
            new Event("Mary", "./home", 1000L),
            new Event("Bob", "./cart", 2000L),
            new Event("Alice", "./prod?id=100", 3000L),
            new Event("Bob", "./prod?id=1", 3500L),
            new Event("Alice", "./prod?id=200", 3200L),
            new Event("Bob", "./home", 3300L),
            new Event("Bob", "./prod?id=2", 3800L),
            new Event("Bob", "./prod?id=3", 4200L)
        );

        // 定义hosts的列表
        ArrayList<HttpHost> httpHosts = new ArrayList<HttpHost>() {{
            add(new HttpHost("192.168.1.12", 9200));
        }};

        // 定义ElasticsearchSinkFunction
        ElasticsearchSinkFunction<Event> elasticsearchSinkFunction = new ElasticsearchSinkFunction<Event>(){

            @Override
            public void process(Event event,
                                RuntimeContext ctx,
                                RequestIndexer indexer // 发送请求
            ) {
                Map<String,String> map = new HashMap<String, String>();
                map.put(event.user, event.url);

                // 构建一个IndexRequest
                IndexRequest request = Requests.indexRequest()
                    .index("clicks")
                    .type("type")
                    .source(map);

                // 向Es集群发送请求
                indexer.add(request);
            }
        };

        // 写入es
        stream.addSink(new ElasticsearchSink.Builder<>(httpHosts, elasticsearchSinkFunction).build());
        
        env.execute();
    }
}
