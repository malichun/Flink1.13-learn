package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil

/**
 * checkpoint , 就是把RDD持久化
 *
 */
object E06_RDD开发API边缘问题_checkpoint {
    def main(args: Array[String]): Unit = {
        val sc = SparkContextUtil.getSc("checkpoint演示")
        sc.setCheckpointDir("file:///c:/data/123")


        val rdd = sc.parallelize(1 to 10000)

        val rdd2 = rdd.map(i => (i.toString, i))

        val rdd3 = rdd2.map(tp => (tp._1, tp._2 + 10))

        rdd3.cache()

        /**
         * 如果你觉得, 你的这个rdd3非常宝贵, 一单丢失需要重跑的话, 代价太高
         * 那么久可以将rdd3执行checkpoint来持久化保存
         */
        rdd3.checkpoint() // 会额外新增一个job来计算rdd3的数据并存储到HDFS

        rdd3.reduceByKey(_+_).count()

        rdd3.map(tp => (tp._1+"_a", Math.max(tp._2,100))).groupByKey().mapValues(iter=> iter.max).foreach(println)



        sc.stop()
    }
}
