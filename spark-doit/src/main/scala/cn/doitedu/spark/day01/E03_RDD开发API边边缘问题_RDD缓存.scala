package cn.doitedu.spark.day01

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
 *
 */
object E03_RDD开发API边边缘问题_RDD缓存 {
    def main(args: Array[String]): Unit = {
        val sc = SparkContextUtil.getSc("闭包问题")

        val rdd = sc.makeRDD(List("a,b,c", "b,c,d", "f,f,f", "a,d,t"))

        val wordOnePair = rdd.flatMap(_.split(","))
            .map((_,1))

        // cache, 本质上是把RDD运算出来的结果进行物化
//        wordOnePair.cache()
        wordOnePair.persist(StorageLevel.MEMORY_ONLY)
        // rdd缓存的存储级别有如下:
        // NONE 相当于没有存储
        // DISK_ONLY 缓存到磁盘
        // DISK_ONLY_2 缓存到磁盘，2 个副本
        // MEMORY_ONLY 缓存到内存
        // MEMORY_ONLY_2 缓存到内存，2 个副本
        // MEMORY_ONLY_SER 缓存到内存，以序列化格式
        // MEMORY_ONLY_SER_2 缓存到内存，以序列化格式，2 个副本
        // MEMORY_AND_DISK 缓存到内存和磁盘
        // MEMORY_AND_DISK_2 缓存到内存和磁盘，2 个副本
        // MEMORY_AND_DISK_SER 缓存到内存和磁盘，以序列化格式
        // MEMORY_AND_DISK_SER_2 缓存到内存和磁盘，以序列化格式，2 个副本
        // OFF_HEAP 缓存到堆外内存

        // 求每个单词出现的次数
        val wordcount = wordOnePair.reduceByKey(_+_)
        wordcount.collect()

        // 将wordOnePairRDD 这个RDD中的kv对, 按key分组, 然后将value以字符串形式拼接
        val valuesPinjie = wordOnePair.aggregateByKey("")((s, i) => s+i, (s1,s2) => s1+s2)
        valuesPinjie.collect().foreach(println)

        // 假设程序的后续代码中, 再也不用wordOnePair这个RDD了, 我们还可以将之前缓存的数据清除
        wordOnePair.unpersist()


        sc.stop()
    }

}

