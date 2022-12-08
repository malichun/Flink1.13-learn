package cn.doitedu.spark.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author malichun
 * @create 2022/11/27 0027 14:16
 */
object C03_RDD算子_cartesian {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)

        val conf = new SparkConf()
        conf.setMaster("local[2]")
        conf.setAppName("reduceByKey")

        val sc = new SparkContext(conf)

        val rdd1 = sc.parallelize(Seq(("a", 12), ("b", 11), ("a", 13), ("b", 14)), 3)
        val rdd2 = sc.parallelize(Seq(("a", "26"), ("a", "20"), ("b", "25"), ("d", "21")))

        val res = rdd1.cartesian(rdd2)

        res.foreach(println)

        sc.stop()
    }
}
