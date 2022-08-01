package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Locale;

/**
 * lambda表达式
 *
 * @author malc
 * @create 2022/8/1 0001 20:05
 */
public class _04_WordCount_LambdaTest {
    public static void main(String[] args) throws Exception {
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        // 读文件, 得到dataStream
        DataStreamSource<String> streamSource = env.readTextFile("FlinkTutorial1.14/flink_course/data/wc/input/wc.txt");

        // 先把一行大写
        SingleOutputStreamOperator<String> upperCased = streamSource.map(String::toUpperCase);

        // 然后切成单次, 并转成(单词 ,1),并压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> returns = upperCased
            .<Tuple2<String, Integer>>flatMap((line, out) -> {
                String[] arr = line.split(" ");
                Arrays.stream(arr).forEach(w -> out.collect(Tuple2.of(w, 1)));
            })//.returns(Types.TUPLE(Types.STRING, Types.INT));
            //.returns(new TypeHint<Tuple2<String, Integer>>() {});
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));


        // 按单词分组
        returns
            .keyBy(e -> e.f0)
            .sum(1)
            .print();

        // 统计单词个数


        env.execute();

    }
}
