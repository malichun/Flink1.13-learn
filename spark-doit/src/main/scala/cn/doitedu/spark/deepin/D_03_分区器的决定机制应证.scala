package cn.doitedu.spark.deepin

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

case class Stu(id:Int, name:String, gender:String, score:Double)

object D_03_分区器的决定机制应证 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setMaster("local[*]")
        conf.setAppName("aaaa")
        conf.set("spark.default.parallelism", "4")

        val sc = new SparkContext(conf)

//        val rdd = sc.hadoopFile("data/wordcount/input",classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
        val rdd = sc.makeRDD(1 to 10 )
        println(rdd.partitioner) // None

        val rdd2 = rdd.map((_,1))
        println(rdd2.partitioner) // None

        // shuffle所产生的RDD通常都有分区器, 而且默认分区器都是HashPartitioner
        val rdd31 = rdd2.reduceByKey(_+_,3) //reduceByKey(new HashPartitioner(numPartitions), func)
        val rdd32 = rdd2.reduceByKey(new HashPartitioner(3),_+_)
        val rdd3 = rdd2.reduceByKey(_+_) // reduceByKey(defaultPartitioner(self), func), 这边是4

        println(rdd3.partitioner)  // Some(org.apache.spark.HashPartitioner@4)
        println("没传任何分区数相关参数的reduceByKey的分区数结果过: "+rdd3.partitioner.get.numPartitions)  // 4





        val rddStu = sc.makeRDD(Seq(
            Stu(1, "a","m", 97),
            Stu(2, "b","f", 98),
            Stu(3, "c","m", 92),
            Stu(4, "d","f", 96),
            Stu(5, "e","f", 98),
        ))

        // 在需要partitioner的算子中, 不传分区器, 算子底层会生成一个默认分区器
        rddStu.groupBy(_.gender).foreach(println) // HashPartitioner

        val rddGrouped = rddStu.groupBy(stu => stu, new HashPartitioner(4){
            override def getPartition(key: Any): Int = {
                super.getPartition(key.asInstanceOf[Stu].gender)
            }
        })

        println(rddGrouped.partitioner) //Some(cn.doitedu.spark.deepin.D_03_分区器的决定机制应证$$anon$1@4)

        //  this.keyBy[K](f)  //
        //        .sortByKey(ascending, numPartitions)   // ShuffledRDD
        //        .values   // 变成mapPartitionsRDD
        val rddSorted = rddStu.sortBy(stu => stu.score)
        println(rddSorted.partitioner)  // None, 先转成tuple, 然后sortByKey, 后再mapValues, 将Partitioner弄没了

        val rddSorted2 = rddStu.map(stu =>(stu.score, stu)).sortByKey()
        println(rddSorted2.partitioner) // Some(org.apache.spark.RangePartitioner@16b5dc50)

        sc.stop()
    }
}
