package cn.doitedu.spark.day01

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author malichun
 * @create 2022/11/27 0027 0:32
 */
object B11_RDD算子_aggregateByKey {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)

        val conf = new SparkConf()
        conf.setMaster("local[2]")
        conf.setAppName("reduceByKey")

        val sc = new SparkContext(conf)

        val rdd = sc.parallelize(Seq(("a",2), ("b",1),("a",3),("b",4),("c",1),("a",6),("b",6)),2)

        // 需求1: 将相同key的数据进行累加
        val rdd2 = rdd.aggregateByKey(100)((u, e) => u + e, (u1, u2) => u1 + u2)

        // 需求2,将相同kye的元素聚合成一个List
        val rdd3 = rdd.aggregateByKey(List[Int]())((u, e) => u.::(e), _ ::: _)

//        rdd2.collect().foreach(println)

        rdd3.foreach(println)

    }
}
