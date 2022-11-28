package cn.doitedu.spark.day01

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.{HashPartitioner, Partitioner}

/**
 * @author malichun
 * @create 2022/11/28 0028 0:16
 */
case class Order(id: Int, uid: Int, amount: Double)

object D02_RDD算子_分区调整 {
    def main(args: Array[String]): Unit = {
        val sc = SparkContextUtil.getSc("分区调整")
        val rdd = sc.parallelize(1 to 10, 4)
        val rddkv1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)), 2)
        val rddkv2 = sc.parallelize(Seq(
            (Order(1, 10, 100), 1),
            (Order(2, 10, 80), 1),
            (Order(3, 12, 100), 1),
            (Order(2, 10, 200), 1),
            (Order(3, 12, 180), 1)
        ), 2)

        // coalesce
        val rdd1 = rdd.coalesce(2, false)
        val rdd12 = rdd.coalesce(6, true)

        // repartition 底层调用的是 coalesce(?, true)
        val rdd3 = rdd.repartition(2)
        val rdd4 = rdd.repartition(10)

        // partitionBy 可以传入用户自定义传入Partitioner
        rddkv1
            .partitionBy(new HashPartitioner(3)) // HashPartitioner 是将key的hashCode%分区数 来决定一条数据该分到哪个分区

        // 如果key是一个复杂对象, 而且要按对象中的某个属性来分区, 则需要自己写分区逻辑
        rddkv2
            .partitionBy(new OrderPartitioner(2))


    }
}

class OrderPartitioner(val num:Int) extends HashPartitioner(num) {
    override def numPartitions: Int = num

    override def getPartition(key: Any): Int = {
        val od = key.asInstanceOf[Order]
        Math.abs(od.id.hashCode() % numPartitions)
        super.getPartition(od.id)
    }
}
