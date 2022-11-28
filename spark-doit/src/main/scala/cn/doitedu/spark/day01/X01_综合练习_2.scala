package cn.doitedu.spark.day01

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 学生信息
 * 1,170,75,87,m
 * 2,175,80,82,m
 * 3,168,72,77,m
 * 4,165,55,97,f
 * 5,160,52,95,f
 * 6,162,48,98,f
 *
 * test
 * a,176,86,79
 * b,162,48,96
 *
 * 根据给定的经验样本数据, 判断他们的性别, 预估性别
 *
 * 看成坐标点
 *
 */
object X01_综合练习_2 {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN) // "org.apache.spark"
        val conf = new SparkConf()
        conf.setAppName("联系")
        conf.setMaster("local[2]")
        val sc = new SparkContext(conf)

        // 加载样本数据集

        val sampleData = sc.makeRDD(List("1,170,75,87,m", "2,175,80,82,m", "3,168,72,77,m", "4,165,55,97,f", "5,160,52,95,f", "6,162,48,98,f"))

        // 数据加工
        val sample = sampleData.map(line => {
            val arr = line.split(",")
            // (id, 性别, 属性数组)
            (arr(0), arr(4), List(arr(1).toDouble, arr(2).toDouble, arr(3).toDouble))
        })

        // 加载测试数据集
        val testData = sc.makeRDD(List("a,176,86,79", "b,162,48,96"))
        val test = testData.map(line => {
            val arr = line.split(",")
            // (id, 属性数组)
            (arr(0), List(arr(1).toDouble, arr(2).toDouble, arr(3).toDouble))
        })

        // 将两份数据做笛卡尔积
        val tmp = test.cartesian(sample)
        //        tmp.foreach(println)
        // ((a,List(176.0, 86.0, 79.0)),(4,f,List(165.0, 55.0, 97.0)))
        // ((a,List(176.0, 86.0, 79.0)),(5,f,List(160.0, 52.0, 95.0)))
        // ((a,List(176.0, 86.0, 79.0)),(1,m,List(170.0, 75.0, 87.0)))
        // ((a,List(176.0, 86.0, 79.0)),(6,f,List(162.0, 48.0, 98.0)))
        // ((a,List(176.0, 86.0, 79.0)),(2,m,List(175.0, 80.0, 82.0)))
        // ((a,List(176.0, 86.0, 79.0)),(3,m,List(168.0, 72.0, 77.0)))
        // ((b,List(162.0, 48.0, 96.0)),(1,m,List(170.0, 75.0, 87.0)))
        // ((b,List(162.0, 48.0, 96.0)),(2,m,List(175.0, 80.0, 82.0)))
        // ((b,List(162.0, 48.0, 96.0)),(3,m,List(168.0, 72.0, 77.0)))
        // ((b,List(162.0, 48.0, 96.0)),(4,f,List(165.0, 55.0, 97.0)))
        // ((b,List(162.0, 48.0, 96.0)),(5,f,List(160.0, 52.0, 95.0)))
        // ((b,List(162.0, 48.0, 96.0)),(6,f,List(162.0, 48.0, 98.0)))

        // 求未知性别的人,与每一个已知性别的样本, 求距离
        val tmp2 = tmp.map { case ((id1, list1), (id2, gender, list2)) =>
            // 取出测试人员的属性数据
            // 去除样本人员的属性数据
            // 套公式:
            val dist = list1.zip(list2).map(t => Math.pow(t._1 - t._2, 2.0)).sum // 距离的平方
            // 未知人id, 样本性别, 距离的平方
            (id1, gender, dist)
        }
        // tmp2.foreach(println)
        // (a,m,221.0)
        // (a,f,1406.0)
        // (a,m,46.0)
        // (a,f,1668.0)
        // (a,m,264.0)
        // (a,f,2001.0)
        // (b,m,874.0)
        // (b,m,1389.0)
        // (b,m,973.0)
        // (b,f,59.0)
        // (b,f,21.0)
        // (b,f,4.0)

        // 从tmp2中取每个未知性别人距离最近的样本性别
        tmp2.map(t => (t._1, t))
            .reduceByKey((t1, t2) => {
                if (t1._3 < t2._3) t1 else t2 // 每个分区取最小值, 然后分区间再取最小值
            })
            .foreach(println)
        //(a,(a,m,46.0))
        //(b,(b,f,4.0))


        sc.stop()
    }
}
