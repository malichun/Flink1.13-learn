package cn.doitedu.flink.flinksql.demos;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Locale;

/**
 * 度量
 * @author malichun
 * @create 2022/08/20 0020 16:56
 */
public class Demo25_MetricDemos {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> ds = env.socketTextStream("hadoop102",8888);

        ds.process(new ProcessFunction<String, String>() {
            LongCounter longCounter;
            MyGauge gauge;

            @Override
            public void open(Configuration parameters) throws Exception {
                longCounter = getRuntimeContext().getLongCounter("doitedu-counter");
                gauge = getRuntimeContext().getMetricGroup().gauge("doitedu-gauge", new MyGauge());
            }

            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                // 业务逻辑之外的metric代码, 度量task所输入的数据条数
                longCounter.add(1);

                gauge.add(1);

                out.collect(value.toUpperCase(Locale.ROOT));
            }
        }).print();

        env.execute();
    }

    // Gauge: 计量表, 仪表
    public static class MyGauge implements Gauge<Integer>{
        int recordCount = 0;

        public void add(int i){
            recordCount += i;
        }

        @Override
        public Integer getValue() {
            return recordCount;
        }
    }
}
