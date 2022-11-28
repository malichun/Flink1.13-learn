package cn.doitedu.spark.day01

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author malichun
 * @create 2022/11/27 0027 13:56
 */
object B13_RDD算子_Coalesce {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)

        val conf = new SparkConf()
        conf.setMaster("local[2]")
        conf.setAppName("reduceByKey")

        val sc = new SparkContext(conf)

        val rdd = sc.parallelize(Seq(("a", 2), ("b", 1), ("a", 3), ("b", 4), ("c", 1), ("a", 6), ("b", 6), ("d", 5)), 3)
        println(rdd.partitions.length) // 3

        // coalesce 可以改成RDD的分区数
        // 当shuffle=true时, 可以将原来的rdd的分区数变多和变少
        // 当shuffle=false时, 稚嫩工匠原来的rdd的分区数变少(哪怕给一个更多的分区数, 哪怕给更多的分区数,实际结果只会保留原来的分区数)
        val rdd2 = rdd.coalesce(6, false)

        println(rdd2.partitions.length) // 3

        println("------------------------------")

        /**
         * repartition 的实质是调用了coalesce
         * coalesce(numPartitions, shuffle= true) // shuffle被设置成true
         * 所以,repartition既可以将分区数改大,也可以将分区数改小(repartition 一定伴随着shuffle的发生)
         */
        val rdd3 = rdd.repartition(10)
        println(rdd3.partitions.size)

        val rdd4 = rdd.repartition(2)
        println(rdd4.partitions.size)

        sc.stop()

    }
}
