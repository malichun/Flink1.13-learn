package cn.doitedu.spark.day01

import cn.doitedu.spark.util.SparkContextUtil

import scala.collection.mutable

/**
 * spark中这个所谓的"闭包", 只是看起来类似各种编程语言中的闭包
 * 本质上根本不是一回是
 * spark中的这个"闭包", 其实是Driver把分布式算子中引用的外部变量序列化后, 发送给每个task来使用
 * 闭包引用的目标对象, 必须是可序列化的!!!而且数据量不能太大
 */
object E03_RDD开发API边边缘问题_闭包 {
    def main(args: Array[String]): Unit = {
        val sc = SparkContextUtil.getSc("闭包问题")

        val rdd = sc.parallelize(Seq((1,"北京"), (2, "上海")))

        // 这个map对象是在Driver程序中创建的
        val mp = new mutable.HashMap[Int,String]()
        mp.put(1,"张三")
        mp.put(2,"李四")

        // object对象
        val data = JobName

        /**
         * map端join
         */
        // rdd的计算时再集群中的task程序中运行
        val rdd2 =rdd.map(tp => {
            // 分布式算子中的函数, 如果引用的外部的变量, 则driver会把该变量序列化后通过网络发送给每一个task
            // 普通对象在每个task线程中都持有一份, 不存在任何线程安全问题
            val name = mp(tp._1)

            // 单例对象, 只在每个executor中持有一份, 由executor中多个task线程共享
            // 不要在这里对该变量进行任何修改操作, 否则会产生线程安全问题
            val jobName = data.job

            // 闭包函数内, 对外部变量做了修改
            mp.put(1, "张三三")

            (tp._1, tp._2, name, jobName)
        })

        rdd2.foreach(println)

        // 闭包函数外, 还是driver端
        // driver端这个对象是没有发生任何改变的, 因为算子里面的修改动作是发生在task中执行, task中对自己持有的"拷贝"做了修改
        println(mp) // 张三

        sc.stop()
    }

}

object JobName extends Serializable {
    val id:Int = 1
    val job:String = "员工"
}
