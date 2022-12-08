package cn.doitedu.spark.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author malichun
 * @create 2022/11/27 0027 14:16
 */
object C02_RDD算子_Cogroup {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)

        val conf = new SparkConf()
        conf.setMaster("local[2]")
        conf.setAppName("reduceByKey")

        val sc = new SparkContext(conf)

        val rdd1 = sc.parallelize(Seq(("a", 12), ("b", 11), ("a", 13), ("b", 14)), 3)
        val rdd2 = sc.parallelize(Seq(("a", "26"), ("a", "20"), ("b", "25"), ("d", "21")))

        val res = rdd1.cogroup(rdd2)

        res.foreach(println)
        //(d,(CompactBuffer(),CompactBuffer(21)))
        //(b,(CompactBuffer(11, 14),CompactBuffer(25)))
        //(a,(CompactBuffer(12, 13),CompactBuffer(26, 20)))

        sc.stop()
    }
}
