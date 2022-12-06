package cn.doitedu.spark.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
 * @author malichun
 * @create 2022/11/27 0027 0:32
 */
object B10_RDD算子_reduceByKey {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)

        val conf = new SparkConf()
        conf.setMaster("local[2]")
        conf.setAppName("reduceByKey")

        val sc = new SparkContext(conf)

        val rdd = sc.parallelize(Seq(("a",2), ("b",1),("a",3),("b",4),("c",1),("a",6),("b",6)),2)


        // reduceByKey
        val rdd2 = rdd.reduceByKey(_ + _)

        // 如下这个写法得到的结果与上面的reduceByKey完全相同
        rdd.groupByKey().map(tp => (tp._1, tp._1.sum))

        /**
         * 如果要实现上面的同样的需求, 用reduceByKey比 "用groupByKey后再聚合效率更高"
         *
         */

        rdd2 foreach println

    }
}
