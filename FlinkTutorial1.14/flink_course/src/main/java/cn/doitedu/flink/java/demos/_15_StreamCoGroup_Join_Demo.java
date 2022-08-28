package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * @author malichun
 * @create 2022/08/28 0028 21:00
 */
public class _15_StreamCoGroup_Join_Demo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        // id,name
        DataStreamSource<String> stream1 = env.socketTextStream("hadoop02", 9998);
        SingleOutputStreamOperator<Tuple2<String, String>> s1 = stream1.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.of(arr[0], arr[1]);
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));


        // id,age, city
        DataStreamSource<String> stream2 = env.socketTextStream("hadoop02", 9999);

        SingleOutputStreamOperator<Tuple3<String, String, String>> s2 = stream2.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1], arr[2]);
        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING));

        // 流的cogroup
        DataStream<String> resultStream = s1.coGroup(s2)
            .where(tp -> tp.f0) // 左流的 f0 字段
            .equalTo(tp -> tp.f0) // 右流的f0字段
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))// 划分窗口
            .apply(new CoGroupFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() { // 处理逻辑
                /**
                 *
                 * @param first 是协同组中第一个流中的元素
                 * @param second 是协同组中第二个流的元组
                 * @param out 是协处理结构
                 * @throws Exception
                 */
                @Override
                public void coGroup(Iterable<Tuple2<String, String>> first, Iterable<Tuple3<String, String, String>> second, Collector<String> out) throws Exception {
                    // 实现左外连接---
                    for (Tuple2<String, String> t1 : first) {
                        boolean flag = false;
                        for (Tuple3<String, String, String> t2 : second) {
                            flag = true;
                            out.collect(t1.f0 + "," + t1.f1 + "," + t2.f0 + "," + t2.f1 + "," + t2.f2);
                        }
                        if (!flag) {
                            // 如果能走到这里面, 说明右表没有数据, 则直接输出左表数据
                            out.collect(t1.f0 + "," + t1.f1 + ",null,null,null");
                        }
                    }
                }
            });

        //resultStream.print();

        ///////////////////////////////////////////////////////
        /**
         * 流的join算子
         * 实例背景:
         *  流1数据 : id,name
         *  流2数据: id,name,city
         *  利用join算子,来实现两个流的数据按id关联
         *  (只有inner join)
         */
        DataStream<String> resultStream2 = s1.join(s2)
            .where(tp2 -> tp2.f0)
            .equalTo(tp3 -> tp3.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .apply(new FlatJoinFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {
                @Override
                public void join(Tuple2<String, String> t1, Tuple3<String, String, String> t2, Collector<String> out) throws Exception {
                    out.collect(t1.f0 + "," + t1.f1 + "," + t2.f0 + "," + t2.f1 + "," + t2.f2);
                }
            });

        resultStream2.print();
        env.execute();
    }
}
