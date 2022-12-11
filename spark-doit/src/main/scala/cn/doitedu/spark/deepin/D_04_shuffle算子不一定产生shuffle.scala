package cn.doitedu.spark.deepin

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.HashPartitioner

/**
 * @author malichun
 * @create 2022/12/11 0011 20:55
 */
object D_04_shuffle算子不一定产生shuffle {
    def main(args: Array[String]): Unit = {
        val sc = SparkContextUtil.getSc("shuffle算子是否产生shuffle")

        val rdd1 = sc.parallelize(Seq(
            ("a",2),
            ("a",1),
            ("b",3),
            ("c",2),
            ("a",5)
        ))

        val rdd2 = sc.parallelize(Seq(
            ("a",2),
            ("a",1),
            ("b",3),
            ("c",2),
            ("a",5)
        ))

        // rdd1.join(rdd2)  肯定有shuffle

        // rdd1.reduceByKey(_+_) 肯定有shuffle

        val rdd12 = rdd1.partitionBy(new HashPartitioner(2))
        val rdd13 = rdd12.reduceByKey(_+_) // 没有shuffle,因为rdd12的分区器是HashPartitioner(2),而此算子底层传入的分区器是父RDD的这个HashPartitioner
        rdd13.count

        val rdd14 = rdd1.repartition(2) // 有shuffle, repartition把partition丢了
        val rdd15 = rdd14.reduceByKey(_+_) // 有shuffle
        rdd15.count()


        /**
         * 关于join算子shuffle验证
         */
        val joined1 = rdd1.join(rdd2) // 有shuffle
        joined1.count

        val rdd_a = rdd1.partitionBy(new HashPartitioner(2))
        val rdd_b = rdd2.partitionBy(new HashPartitioner(2))

        val joined2 = rdd_a.join(rdd_b, new HashPartitioner(2)) // 没有shuffle
        val joined3 = rdd_a.join(rdd_b) // 没有shuffle
        val joined4 = rdd_a.join(rdd_b, new HashPartitioner(3)) // 有shuffle,因为父RDD分区器数 != 子RDD分区器数

        joined2.count()
        joined3.count()
        joined4.count()

        Thread.sleep(1000000)

        sc.stop()

    }
}
