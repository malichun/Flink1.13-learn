package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author malichun
 * @create 2022/08/30 0030 23:16
 */
public class _16_BroadCast_Demo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        //id, eventId
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9998);
        SingleOutputStreamOperator<Tuple2<String, String>> s1 = stream1.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.of(arr[0], arr[1]);
        }).returns(new TypeHint<Tuple2<String, String>>() {
        });

        // id,age,city
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple3<String, String, String>> s2 = stream2.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1], arr[2]);
        }).returns(new TypeHint<Tuple3<String, String, String>>() {
        });

        /**
         * 案例背景
         *  流1: 用户行为事件流(持续不断, 同一个人也会反复出现, 出现次数不定)
         *  流2: 用户的维度信息(年龄,城市), 同一个人的数据只会来一次, 来的时间也不定(作为广播流)
         *
         *  需要加工流1, 把用户的维度信息填充好, 利用广播流来实现
         */

        // 将字典数据所在流 s2 转成广播流
        MapStateDescriptor<String, Tuple2<String, String>> mapStateDescriptor = new MapStateDescriptor<>("userInfoState", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        }));
        BroadcastStream<Tuple3<String, String, String>> s2BroadcastStream = s2.broadcast(mapStateDescriptor);

        // 哪个流处理中需要用到广播状态数据, 就要去 连接 这个广播流
        SingleOutputStreamOperator<String> process = s1.connect(s2BroadcastStream)
            .process(new BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {
                final MapStateDescriptor<String, Tuple2<String, String>> mapStateDescriptor = new MapStateDescriptor<>("userInfoState", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}));

                /**
                 * 本方法是用来处理主流中的数据,每来一条调用一次
                 * @param value 主流中的一条数据
                 * @param ctx 上下文
                 * @param out 输出器
                 * @throws Exception
                 */
                @Override
                public void processElement(Tuple2<String, String> value, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                    // 通过ReadOnlyContext ctx取到的广播状态是一个只读的
                    ReadOnlyBroadcastState<String, Tuple2<String, String>> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                    Tuple2<String, String> tuple2 = broadcastState.get(value.f0);
                    if(tuple2 == null){
                        out.collect(value.f0 + "," +value.f1);
                    }else {
                        out.collect(value.f0 + "," + value.f1 + "," + tuple2.f0 + "," + tuple2.f1);
                    }
                }

                /**
                 * 处理广播流中的元素
                 * @param value 广播流中的数据
                 * @param ctx 上下文
                 * @param out 输出器
                 * @throws Exception
                 */
                @Override
                public void processBroadcastElement(Tuple3<String, String, String> value, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.Context ctx, Collector<String> out) throws Exception {
                    // 获取到广播状态(现在可以理解为被flink管理着的HashMap)
                    BroadcastState<String, Tuple2<String, String>> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                    // 让后将获得的这条 广播流 数据, 拆分后, 装入广播状态
                    broadcastState.put(value.f0, Tuple2.of(value.f1, value.f2));
                }


            });

        process.print();

        env.execute();
    }
}
