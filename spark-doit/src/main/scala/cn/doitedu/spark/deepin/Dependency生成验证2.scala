package cn.doitedu.spark.deepin

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.hadoop.util.hash.Hash
import org.apache.spark.HashPartitioner

/**
 * @author malc
 * @create 2022/12/8 0008 19:52
 */
object Dependency生成验证2 {
    def main(args: Array[String]): Unit = {

        val sc = SparkContextUtil.getSc("Dependency生成验证")
        val rdd1 = sc.makeRDD(List("a","b","c","a","a","b"))

        val rdd2 = rdd1.flatMap(s => s.split("\\s+"))

        val rdd3 = rdd2.map(w => (w,1))

        val rdd4 = rdd3.partitionBy(new HashPartitioner(2))

        val rdd5 = rdd4.reduceByKey(_+_,2)

        rdd5.collect().foreach(println)

        Thread.sleep(Long.MaxValue)
        sc.stop()
    }

}
