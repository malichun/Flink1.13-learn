package cn.doitedu.spark.day01

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author malichun
 * @create 2022/11/27 0027 14:16
 */
object C01_RDD算子_Join {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)

        val conf = new SparkConf()
        conf.setMaster("local[2]")
        conf.setAppName("reduceByKey")

        val sc = new SparkContext(conf)

        val rdd1 = sc.parallelize(Seq(("a", 12), ("b", 11), ("a", 13), ("b", 14)), 3)
        val rdd2 = sc.parallelize(Seq(("a", "26"),("a","20"), ("b", "25"),  ("d", "21")))

        // 内连接
        // (String, (Int,String))
        val joinedRDD = rdd1.join(rdd2)
        joinedRDD.collect().foreach(println)

        println("===================join =======================")


        // 左外连接, Option两个子类, Some(), None
        // (String,(Int,Option[String])
        val leftJoin = rdd1.leftOuterJoin(rdd2)
        leftJoin.foreach(println(_))


        println("=================右外连接========================")
        val rightJoin = rdd1.rightOuterJoin(rdd2)
        rightJoin.foreach(println)

        println("=================全外连接========================")
        val fullJoin = rdd1.fullOuterJoin(rdd2)
        fullJoin.foreach(println)



        println("================= coGroup ========================")

        val coGroup = rdd1.cogroup(rdd2)
        coGroup.collect().foreach(println(_))


        //练习
        


        sc.stop()
    }
}
