package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.rdd.JdbcRDD

import java.sql.{DriverManager, ResultSet}
import scala.collection.mutable
import scala.io.Source

/**
 * @author malichun
 * @create 2022/12/01 0001 23:17
 * 订单id, 商品分类id, 商品金额
 * 1001,c101,300
 * 1002,c102,200
 * 1003,c101,400
 * 1004,c102,400
 * 1005,c103,600
 * 1006,c102,300
 */
object X03_综合练习3 {
    def main(args: Array[String]): Unit = {
        val sc = SparkContextUtil.getSc("订单分析")

//        val rdd = sc.textFile("data/exersize/tm1_input")
        val source = Source.fromFile("data/exersize/tm1_input/data1.txt")
        val lines = source.getLines().toList
        source.close()

        val rdd = sc.makeRDD(lines)

        // 需求1.每个分类下订单总金额, 并按成交金额排序
        val res1 = rdd.map(s => {
            val arr = s.split(",")
            (arr(1), arr(2).toDouble)
        })
            .reduceByKey(_+_)
            .sortBy(_._2, false)

//        res1.foreach(println)

        //需求2: 将原来的数据, 关联上维度信息, 保存到HDFS中, 补充类别名称字段
        // 期望的结果:
        // 1001, c101, 300, 图书
        // 1002, c102, 200, 化妆品
        // 1003, c101, 400, 图书
        // 1004, c102, 400, 化妆品
        // 1005, c103, 600, 小家电
        // 1006, c102, 300, 化妆品

        /**
         *方式一
         * 在map算子处理每一条原始数据的过程中, 去请求myql查询类别ID对应的类别名称
          */
        val res = rdd.mapPartitions(iter => {
            // 去mysql中查询categoryId对应的categoryIdName
            val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456")
            val stmt = conn.prepareStatement("select name from tb_category where id = ?")

            iter.map(line => {
                val arr = line.split(",")
                val categoryId = arr(1)
               stmt.setString(1, categoryId)
                val rs = stmt.executeQuery()
                if(rs.next()){
                    val categoryName = rs.getString("name")
                    line+","+categoryName
                }else{
                    line+","
                }
            })
        })
//        res.foreach(println)

        /**
         * 方式二:
         * 将mysql中的字典表加载为一个rdd, 和订单RDD进行join
         */
        val conn = () => DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456")
        val sql = "select id, name from tb_category where keyid >= ? and keyid <= ?"
        val mapRow = (rs:ResultSet) => {
            (rs.getString(1),rs.getString(2))
        }
        // 维表RDD, 来自于mysql表
        val dictRDD = new JdbcRDD[(String, String)](sc, conn, sql, 1, 3,2,mapRow)
        //dictRDD.foreach(println)

        // 订单RDD, 来自于订单记录文件
        val orderRDD = rdd.map(s => {
            val arr = s.split(",")
            (arr(1), s)
        })

        val joined = orderRDD.leftOuterJoin(dictRDD)
        val res3 = joined.map { case (s, (s1, opt)) =>
            val ma = opt match {
                case Some(s) => s
                case None => "未知"
            }
            raw"${s1},${ma}"
        }

//        res3.foreach(println)


        /**
         * 方式3:
         * 3.1 先在driver端连接mysql, 读取到维表数据, 然后将维表数据作为广播变量发个各个executor来实现map端join
         * 将维表数据作为广播变量发给各个executor来实现map端join
         */
        // 在driver端用jdbc连接,请求mysql,读取整个表, 保存到一个hashmap中
        val conn1 = DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456")
        val statement = conn1.createStatement()
        val rs = statement.executeQuery("select id,name from tb_category")
        val dictMap = mutable.HashMap[String,String]()
        while(rs.next()){
            val id = rs.getString(1)
            val name = rs.getString(2)
            dictMap.put(id,name)
        }

        // 将Hashmap维表数据广播出去
        val bc = sc.broadcast(dictMap)
        // 开始分布式处理订单数据
       val res4 = rdd.map(line => {
          val arr = line.split(",")
          val categoryId = arr(1)
            val mp = bc.value
            val categoryName = mp.getOrElse(categoryId,"未知")
            line + "," + categoryName
        })

//        res4.foreach(println)

        /**
         * 方式3
         * 3.2 将mysql中的维表加载为一个RDD, 然后将RDD数据通过collect方法收集到driver端的hashmap中
         * 然后将维表数据作为广播变量发给各个executor来实现map端join
         *
         */
        val map2 = dictRDD.collect().toMap


        sc.stop()
    }

}
