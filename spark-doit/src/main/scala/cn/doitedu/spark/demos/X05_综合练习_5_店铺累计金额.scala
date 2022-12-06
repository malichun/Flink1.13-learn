package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil

/**
 * 月累计金额计算
 *
 * shop1,2019-01-18,500
 * shop1,2019-02-10,500
 * shop1,2019-02-10,200
 * shop1,2019-02-11,600
 * shop1,2019-02-12,400
 * shop1,2019-02-13,200
 * shop1,2019-02-15,100
 * shop1,2019-03-05,180
 * shop1,2019-04-05,280
 * shop1,2019-04-06,220
 * shop2,2019-02-10,100
 * shop2,2019-02-11,100
 * shop2,2019-02-13,100
 * shop2,2019-03-15,100
 * shop2,2019-04-15,100
 *
 * 目标结果:
 * shop2,2019-02,300,300
 * shop2,2019-03,400,700
 * shop2,2019-04,300,1000
 */
object X05_综合练习_5_店铺累计金额 {
    def main(args: Array[String]): Unit = {
        val sc = SparkContextUtil.getSc("月累计金额计算")
        val rdd = sc.parallelize(Seq(
            "shop1,2019-01-18,500",
            "shop1,2019-02-10,500",
            "shop1,2019-02-10,200",
            "shop1,2019-02-11,600",
            "shop1,2019-02-12,400",
            "shop1,2019-02-13,200",
            "shop1,2019-02-15,100",
            "shop1,2019-03-05,180",
            "shop1,2019-04-05,280",
            "shop1,2019-04-06,220",
            "shop2,2019-02-10,100",
            "shop2,2019-02-11,100",
            "shop2,2019-02-13,100",
            "shop2,2019-03-15,100",
            "shop2,2019-04-15,100"
        ))


        rdd.map(line => {
            val arr = line.split(",")
            ((arr(0), arr(1).substring(0, 7)), arr(2).toDouble)
        })
            .reduceByKey(_ + _)
            .map { case ((shop, mon), sum) =>
                (shop, (mon, sum))
            }
            .groupByKey()
            .flatMap { shopDetail =>
                val shop = shopDetail._1
                val mon_detail = shopDetail._2
                mon_detail.toList.sortBy(_._1)
                    .scanLeft(("", "", 0.0, 0.0))((res, e: (String, Double)) => {
                        (shop, e._1, e._2, res._4 + e._2)
                    })
                    .tail
            }
            .collect()
            .foreach(println)

        sc.stop()
    }
}
