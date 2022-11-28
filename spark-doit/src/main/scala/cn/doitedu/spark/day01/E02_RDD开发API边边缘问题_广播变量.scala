package cn.doitedu.spark.day01

import cn.doitedu.spark.util.SparkContextUtil

import scala.collection.mutable

/**
 * 广播变量主要应用与需要进行"map端join"的场合
 * 就是把一份小体量的数据, 直接让每个executor持有一份拷贝,在task的计算逻辑中直接可用
 * 而不用两个rdd去join
 *
 * 相关面试题: spark如何实现map端join?
 *  (广播变量, 闭包引用, 缓存文件) // 缓存文件: sc.addFile("hdfs://doit01:8020/abc/names.txt")
 *
 *
 */
object E03_RDD开发API边边缘问题_广播变量 {
    def main(args: Array[String]): Unit = {
        val sc = SparkContextUtil.getSc("闭包问题")


        val rdd = sc.parallelize(Seq(
            (1,"北京"),
            (2, "上海"),
            (3, "上海"))
        )

        // 这个map对象是在Driver程序中创建的
        val mp = Map[Int,String]((1,"张三"), (2, "李四"), (3, "王五"))

        // 将driver端创建的普通集合对象,广播出去
        // 广播的实质是:  将driver端数据对象, 序列化后, 给每个executor发送一份(每个executor只持有一份广播变量的拷贝)
        // 广播变量的数据传输和闭包引用的数据传输有所不同:
        // 闭包引用的数据, 是driver给每个executor直接发送数据
        // 广播变量, 是通过bittorrent协议来发送数据的(所有executor遵循的, 人人为我,我为人人的原则)
        val bc  = sc.broadcast(mp)
        val res = rdd.map(t => {
            val broadValue = bc.value
            val name = broadValue(t._1)
            (t._1, t._2, name)
        })

        res.foreach(println)


        sc.stop()
    }

}

