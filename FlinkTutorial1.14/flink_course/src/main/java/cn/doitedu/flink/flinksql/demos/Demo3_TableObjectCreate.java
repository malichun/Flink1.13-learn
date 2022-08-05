package cn.doitedu.flink.flinksql.demos;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.HashMap;


/**
 * 创建Table对象的n种办法
 */
public class Demo3_TableObjectCreate {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        /* 假设 table_a 已被创建
        tenv.executeSql("create table table_a(id int,name string) " +
            "with ('connector'='kafka'," +
            ")" );
        */


        // TODO 1.从一个已经存在的表名,来创建Table对象
        //Table table_a = tenv.from("table_a");

        // TODO 2.从TableDescriptor来创建table对象
        Table table = tenv.from(TableDescriptor.forConnector("kafka")
            .schema(Schema.newBuilder()  // 指定表结构
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .column("gender", DataTypes.STRING())
                .build()
            )
            .format("json") // 指定数据源的数据格式
            .option("topic", "doit30-2")
            .option("properties.bootstrap.servers", "hadoop102:9092")
            .option("properties.group.id", "g1")
            .option("scan.startup.mode", "earliest-offset")
            .option("json.fail-on-missing-field", "false")
            .option("json.ignore-parse-errors", "true")
            .build());

        // TODO 3.从数据流创建table对象
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers("hadoop102:9092")
            .setTopics("doit30-2")
            .setGroupId("g2")
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStreamSource<String> kfk = env.fromSource(kafkaSource, WatermarkStrategy.forMonotonousTimestamps(), "kfk");
        /*kfk.print();*/

        // TODO 3.1不指定schema, 将流创建成table对象, 表的schema是默认的,往往不符合要求
        Table table1 = tenv.fromDataStream(kfk);
        //table1.execute().print();

        // 为了获得更理想的表结构, 可以先把流数据转成javaBean类型
        SingleOutputStreamOperator<Person> javaBeanStream = kfk.flatMap(new FlatMapFunction<String, Person>() {
            @Override
            public void flatMap(String value, Collector<Person> out) throws Exception {

                try {
                    Person p = JSON.parseObject(value, Person.class);
                    out.collect(p);
                } catch (Exception e) {
                    //e.printStackTrace();
                }
            }
        });

        Table table2 = tenv.fromDataStream(javaBeanStream);
        //table2.execute().print();

        // TODO 4. 手动指定schema定义, 来讲一个javaBean流转成一个Table对象, 这个有问题!!
       /*  Table table3 = tenv.fromDataStream(javaBeanStream, Schema.newBuilder()
            .column("uid", DataTypes.BIGINT()) // 报错, 类型可以变,名字不能变
            .column("name", DataTypes.STRING())
            .column("age", DataTypes.BIGINT())
            .column("sex", DataTypes.STRING())
            .build());

        table3.printSchema();
        table3.execute().print(); */



        //////////////////////////////////////////////////////
        // TODO 5.测试, fromValues

        Table table4 = tenv.fromValues(1, 2, 3, 4, 5);
        table4.printSchema();
        //
        // `f0` INT NOT NULL
        //)
        //table4.execute().print();

        HashMap<String,String> info = new HashMap<>();
        info.put("x","y");

        Table table5 = tenv.fromValues(
            DataTypes.ROW(
                DataTypes.FIELD("id",DataTypes.INT()),
                DataTypes.FIELD("name",DataTypes.STRING()),
                DataTypes.FIELD("info", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
                DataTypes.FIELD("ts1", DataTypes.TIMESTAMP(3)),
                DataTypes.FIELD("ts3",DataTypes.TIMESTAMP_LTZ(3)),
                DataTypes.FIELD("ts4", DataTypes.TIMESTAMP_LTZ(3)),
                DataTypes.FIELD("ts5", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
                DataTypes.FIELD("ts6", DataTypes.TIME(3))
            ),
            Row.of(1, "zs", info, "2022-06-03 13:59:20.200","2022-06-03 13:59:20.200",1654236105000L,1654236105000L,1654236105000L)
        );// row 是个方法

        table5.printSchema();
        table5.execute().print();

        env.execute();

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Person {
        public int id;
        public String name;
        public int age;
        public String gender;
    }
}
