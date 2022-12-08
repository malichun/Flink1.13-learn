package cn.doitedu.spark.deepin

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.{HashPartitioner, SparkContext}

/**
 * @author malc
 * @create 2022/12/8 0008 19:52
 */
object Dependency生成验证 {
    def main(args: Array[String]): Unit = {

        val sc = SparkContextUtil.getSc("Dependency生成验证")
        val rdd1 = sc.makeRDD(List("a","b","c","a","a","b"))

        // rdd2(MapPartitionsRDD) 窄依赖于 rdd1
        val rdd2 = rdd1.flatMap(s => s.split("\\s+"))

        // rdd3(MapPartitionsRDD) 窄依赖于 rdd2
        val rdd3 = rdd2.map(w => (w,1))

        // rdd4(ShuffledRDD) 宽依赖于 rdd3
        val rdd4 = rdd3.reduceByKey(_+_)

        rdd4.collect().foreach(println)

        Thread.sleep(Long.MaxValue)
        sc.stop()
    }

}
