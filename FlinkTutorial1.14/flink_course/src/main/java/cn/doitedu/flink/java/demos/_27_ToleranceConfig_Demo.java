package cn.doitedu.flink.java.demos;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * 状态容错
 * @author malichun
 * @create 2022/09/26 0026 13:38
 */
public class _27_ToleranceConfig_Demo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        /**
         * 在idea做测试的时候, 指定从某个保存点来恢复状态
         */
        conf.setString("","");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE); // 传入两个最基本的参数
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("file:///d:ckpt");
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofMillis(10000)); // 设置ck对其的超时时长
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 设置ck算法模式
        checkpointConfig.setCheckpointInterval(2000); // ck间隔时长
        // checkpointConfig.setCheckpointIdOfIgnoredInFlightData(5); // 用于非对齐算法模式下, 在job恢复时让各个算子自动抛弃ck-5中飞行的数据
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // job cancel掉时,保留最后一次的ck数据
        checkpointConfig.setForceUnalignedCheckpoints(false); // 是否强制使用非对齐的checkpoint模式
        checkpointConfig.setMaxConcurrentCheckpoints(5); // 允许在系统中同时存在飞行中(未完成)的ck数
        checkpointConfig.setMinPauseBetweenCheckpoints(2000); // 设置两次ck之间的最小时间间隔(防止系统的处理资源被ck过于频繁的占用)
        checkpointConfig.setCheckpointTimeout(3000); //一个算子在一次ck执行过程中的总耗费时长上限
        checkpointConfig.setTolerableCheckpointFailureNumber(10); // 允许的checkpoint失败最大次数

        /** ******** 失败重启策略 *************************************************************************** */
        /**
         *
         * Task 失败的自动重启策略
         * 分固定延迟重启, 指数等待时间重启 和 失败率重启
         * 默认没有重启策略
         */
        RestartStrategies.RestartStrategyConfiguration restartStrategy = null;
        // 固定延迟重启(参数1: 故障重启最大次数; 参数2: 2次重启的延迟间隔)
        restartStrategy = RestartStrategies.fixedDelayRestart(5, 2000);

        // 默认的故障重启策略, 不重启(只要有task失败, 整个job就失败)
        restartStrategy = RestartStrategies.noRestart();

        // 指数级别重启
        /**
         * 本策略: 故障越频繁, 两次重启间的惩罚间隔就越长
         * initialBackoff – 重启间隔惩罚的初始值 1s
         * maxBackoff – 重启间隔最大惩罚时长 60s
         * backoffMultiplier – 重启间隔时长的惩罚倍数: 2 (每多故障一次, 重启延迟惩罚就在上一次的惩罚时长上 * 倍数)
         * resetBackoffThreshold – 重置惩罚时长的平稳运行时长阈值(平稳运行达到这个阈值后, 如果再故障, 则故障重启延迟时间重置为了初始值: 1s)
         * jitterFactor – 取一个随机数, 来加在重启时间点上, 以让每次重启的时间点呈现一定的随机性
         *
         */
        restartStrategy = RestartStrategies.exponentialDelayRestart(Time.seconds(1), Time.seconds(60), 2.0, Time.hours(1), 1.0);

        /**
         * @param failureRate 在指定时长内的最大失败次数
         * @param failureInterval 指数衡量时长
         * @param delayInterval 两次重启之间的时间间隔
         */
        restartStrategy = RestartStrategies.failureRateRestart(5, Time.hours(1), Time.seconds(5));

        // 本策略就是退回到配置文件所配置的策略
        // 常用语自定义 RestartStrategy
        // 用户自定义了重启策略类, 常常配置在了flink-conf.yaml文件中: 本策略就是 退回到配置文件所配置的策略
        RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration = RestartStrategies.fallBackRestart();

        env.setRestartStrategy(restartStrategy);


        /** *********************************************************************************** */


        DataStreamSource<String> source = env.socketTextStream("localhost",9999);

        env.execute();
    }
}
