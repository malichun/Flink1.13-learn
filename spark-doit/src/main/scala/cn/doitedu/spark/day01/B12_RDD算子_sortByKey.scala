package cn.doitedu.spark.day01

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


/**
 *
 * sortByKey全局排序, 分区间根据key有序, 分区内根据key排序
 * @author malichun
 * @create 2022/11/27 0027 0:32
 */
object B12_RDD算子_sortByKey {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)

        val conf = new SparkConf()
        conf.setMaster("local[2]")
        conf.setAppName("reduceByKey")

        val sc = new SparkContext(conf)

        val rdd = sc.parallelize(Seq(("a",2), ("b",1),("a",3),("b",4),("c",1),("a",6),("b",6),("d",5)),2)

        val rdd2 = rdd.sortByKey() // 默认升序
        rdd2.cache()

        rdd2.mapPartitionsWithIndex((index,iter) => {
           iter.map(e => (index, e))
        }).collect().foreach(println)
        //(0,(a,2))
        //(0,(a,3))
        //(0,(a,6))
        //(0,(b,1))
        //(0,(b,4))
        //(0,(b,6))
        //(1,(c,1))
        //(1,(d,5))



        sc.stop()
    }
}
