package cn.doitedu.spark.deepin

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author malc
 * @create 2022/12/9 0009 19:16
 */
object D_02_默认并行度策略验证 {
    def main(args: Array[String]): Unit = {
        /* val sc = SparkContextUtil.getSc("D02_默认并行度策略验证", "local[*]")
            sc.getConf.set("spark.default.parallelism","4")
        * */
        val conf = new SparkConf()
        conf.setMaster("local[*]")
        conf.setAppName("aaaa")
        conf.set("spark.default.parallelism", "4")


        // 没有传入分区数, 底层就走的默认并行度计算策略来的到默认分区数

        // 对应local运行模式:  LocalSchedulerBackend.defaultParallelism()
        // 核心逻辑: scheduler.conf.getInt("spark.default.parallelism",totalCores)

        // 对应分布式集群运行模式: CoarseGrainedSchedulerBackend.defaultParallelism()
        // 核心逻辑: conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))


        // 构造sc之前设置并行度
        val sc = new SparkContext(conf)
        val rdd = sc.parallelize(1 to 10000)



        println(rdd.partitions.size)  // 结果: 4

        sc.stop()
    }
}
