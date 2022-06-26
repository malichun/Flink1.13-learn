package com.atguigu.chapter05_StreamAPI;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author malichun
 * @create 2022/6/24 21:41
 */
public class Sink05ToJDBC {
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


        stream.addSink(JdbcSink.sink("INSERT INTO clicks(user,url) VALUES(? ,?)",
            new JdbcStatementBuilder<Event>() {
                @Override
                public void accept(PreparedStatement preparedStatement, Event event) throws SQLException {
                    preparedStatement.setString(1, event.user);
                    preparedStatement.setString(2, event.url);
                }
            },

            JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(1000)
                .withMaxRetries(5)
                .build(),

            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://192.168.1.12:3306/test")
                .withDriverName("com.mysql.jdbc.Driver")
                .withUsername("root")
                .withPassword("root")
                .build()
                ));


        env.execute();
    }
}
