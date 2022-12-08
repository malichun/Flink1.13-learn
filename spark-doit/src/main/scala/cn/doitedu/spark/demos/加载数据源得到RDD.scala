package cn.doitedu.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author malichun
 * @create 2022/10/13 0013 11:47
 */
object 加载数据源得到RDD {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        // 指定运行模式
        conf.setAppName("三国志")
        // 指定运行模式
        conf.setMaster("local[*]")
        // 构造spark编程入口对象
        val sc = new SparkContext(conf)

        // 加载文本数据, 得到起始RDD(弹性分布式数据集)
        val rdd1 = sc.textFile("data/battel/input/")

        // 打印rdd1中的元素
        rdd1.foreach(println)

//        // Seq文件, 就是k,v文件
//        val rdd2: RDD[(Int, String)] = sc.sequenceFile("data/seq/input", classOf[Int], classOf[String])
//        rdd2.collect().foreach(println)

        sc.stop()
    }
}
