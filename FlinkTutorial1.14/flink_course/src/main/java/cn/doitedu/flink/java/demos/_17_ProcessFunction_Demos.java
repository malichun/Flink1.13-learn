package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Locale;

/**
 * @author malichun
 * @create 2022/08/31 0031 21:14
 */
public class _17_ProcessFunction_Demos {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8822);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        //id, eventId
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9998);
        SingleOutputStreamOperator<Tuple2<String, String>> s1 = stream1.process(new ProcessFunction<String, Tuple2<String, String>>() {

            // 可以使用声明周期 open 方法
            @Override
            public void open(Configuration parameters) throws Exception {
                // 可以调用哦geRuntimeContext方法拿到各种运行时上下文信息
                RuntimeContext runtimeContext = getRuntimeContext();
                runtimeContext.getTaskName();
                super.open(parameters);
            }

            @Override
            public void processElement(String value, ProcessFunction<String, Tuple2<String, String>>.Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
                // 可以做测流输出
                ctx.output(new OutputTag<>("s1", Types.STRING), value);

                // 可以做主流输出
                String[] split = value.split(",");
                out.collect(Tuple2.of(split[0], split[1]));
            }

            // 可以使用声明周期 close 方法
            @Override
            public void close() throws Exception {
                super.close();
            }
        });

        // 在普通的dataStream上调用process算子, 传入的是ProcessFunction
        KeyedStream<Tuple2<String, String>, String> keyedStream = s1.keyBy(tp2 -> tp2.f0);
        // 在keyedStream上调用Process,传入的是KeyedProcessFunction
        keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<Integer,String>>() { // <K,I,O>
            @Override
            public void processElement(Tuple2<String, String> value, KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<Integer,String>>.Context ctx, Collector<Tuple2<Integer,String>> out) throws Exception {
                // 把id变整数, 把eventId变大写
                out.collect(Tuple2.of(Integer.parseInt(value.f0), value.f1.toUpperCase(Locale.ROOT)));

            }
        });

        // id,age,city
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple3<String, String, String>> s2 = stream2.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1], arr[2]);
        }).returns(new TypeHint<Tuple3<String, String, String>>() {
        });





        env.execute();
    }
}
