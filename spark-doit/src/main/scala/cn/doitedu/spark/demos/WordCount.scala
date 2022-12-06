package cn.doitedu.spark.demos

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author malichun
 * @create 2022/10/13 0013 0:25
 */
object WordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

        // 构造一个spark编程入口对象
        val sc = new SparkContext(conf)

        // 加载数据源
        val rdd = sc.textFile("data/wordcount/input/a.txt")

        // 调用各种转换(transformation)算子
        val value = rdd.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _)

        // 触发执行(行动action算子)
        value.collect().foreach(println)

        sc.stop()
    }
}
