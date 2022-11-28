package cn.doitedu.spark.day01

import cn.doitedu.spark.util.SparkContextUtil

import java.sql.DriverManager

case class Person(id:Int, age:Int, salary:Double)

/**
 * @author malichun
 * @create 2022/11/27 0027 23:29
 */
object D01_RDD算子_行动算子 {
    def main(args: Array[String]): Unit = {
        val sc = SparkContextUtil.getSc("行动算子测试")

        val rdd1 = sc.parallelize(Seq("a a a a a b", "c c c a b c a c", " nd a f g a c d"))
        val rdd2 = sc.parallelize(1 to 10)
        val rdd3 = sc.makeRDD(Seq(Person(1,18,9888), Person(2,28,6800),Person(3,24,12000)))

        // reduce 将RDD中的所有数据都聚合成一个值
        val res = rdd2.reduce(_ + _)
        println(res)
        // 55

        // collect 将rdd中的数据汇总到driver端
        // 本算子慎用(因为一个rdd中数据体量庞大, 汇总给到driver端很容易引起内存不够)
        val collect = rdd1.collect()

        // count 计算RDD中数据的条数
        val cnt = rdd1.count()
        println(cnt)

        // first 取RDD中的第一条数据
        val str = rdd1.first()
        val i = rdd2.first()

        // take(n) 从rdd中取n条数据
        val takeStr: Array[String] = rdd1.take(2)

        // taskSample() 随机抽样,并返回样本数据
        val sample = rdd2.takeSample(withReplacement = false, 2)

        // takeOrdered 按顺序取的
        val strings: Array[String] = rdd1.takeOrdered(5)

            // 隐式参数
        implicit val personOrdering = new Ordering[Person]{
            override def compare(x: Person, y: Person) = x.age.compare(y.age)
        }
        rdd3.takeOrdered(3).foreach(println)


        // countByKey() 统计rdd中每个key的数据条数
        val rdd4 = rdd1.flatMap(line => {
            line.split("\\s+").map((_, 1))
        })
        val wc = rdd4.reduceByKey(_+_)  // 转换算子
        val wordcount: collection.Map[String, Long] = rdd4.countByKey()  // action算子


        // foreach 对rdd中每条数据执行一个指定的动作(函数)
        rdd1.foreach(s => {
//            // 连接
//            val conn = DriverManager.getConnection("","root","")
//            val stmt = conn.prepareStatement();
        })

        // foreachPartition 类似 foreach, 但是会一次性给你振哥分区数据的迭代器
        rdd1.foreachPartition(iter => {
            val conn = DriverManager.getConnection("","root","")
            val stmt = conn.prepareStatement("insert into t value(?)")

            iter.foreach(s => {
                stmt.setString(1,s)
                stmt.execute()
            })
        })

        // saveAsTextFile 将rdd的数据以文本文件的格式保存到文件系统中
        rdd1.saveAsTextFile("data/saveastextfile/")
        rdd1.saveAsTextFile("hdfs://doit01:8020/data/saveastextfile/")

        sc.stop()
    }
}
