package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.CheckPointLoader

/**
 * 使用上面生成的checkpoint
 */
object E06_RDD开发API边缘问题_checkpoint2 {
    def main(args: Array[String]): Unit = {
        val sc = SparkContextUtil.getSc("checkpoint演示")
        sc.setCheckpointDir("file:///c:/data/123")

        // 加载checkpoint的, 反序列化为rdd
        val rdd3 = CheckPointLoader.load(sc,"file:///C:\\data\\123\\6169587f-d27d-41c4-a06a-6d507d176c48\\rdd-2")

        println(rdd3.reduceByKey(_ + _).count())

        rdd3.map(tp => (tp._1+"_a", Math.max(tp._2,100))).groupByKey().mapValues(iter=> iter.max).foreach(println)



        sc.stop()
    }
}
