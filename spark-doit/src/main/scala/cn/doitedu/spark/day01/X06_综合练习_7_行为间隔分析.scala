package cn.doitedu.spark.day01

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.commons.lang3.time.DateUtils

import java.sql.Date
import scala.collection.mutable.ListBuffer

/**
 * 行为间隔分析
 * 流量统计: 将同一个用户的多个上网行为数据进行聚合, 如果两次上网相邻时间小于10分钟, 就累计到一起
 * 用户  起始时间            结束时间
 * 1,2020-02-18 14:20:30,2020-02-18 14:46:30,20
 * 1,2020-02-18 14:47:20,2020-02-18 15:20:30,30
 *
 * 1,2020-02-18 15:37:23,2020-02-18 16:05:26,40
 * 1,2020-02-18 16:06:27,2020-02-18 17:20:49,50
 * 1,2020-02-18 17:21:50,2020-02-18 18:03:27,60
 *
 * 2,2020-02-18 14:18:24,2020-02-18 15:01:40,20
 * 2,2020-02-18 15:20:49,2020-02-18 15:30:24,30
 * 2,2020-02-18 16:01:23,2020-02-18 16:40:32,40
 * 2,2020-02-18 16:44:56,2020-02-18 17:40:52,50
 * 3,2020-02-18 14:39:58,2020-02-18 15:35:53,20
 * 3,2020-02-18 15:36:39,2020-02-18 15:24:54,30
 */
case class Action(uid: String, startTime: String, endTime: String, num:Int)

object X06_综合练习_7_行为间隔分析 {
    def main(args: Array[String]): Unit = {
        val sc = SparkContextUtil.getSc("流量统计")

        val rdd = sc.makeRDD(List(
            "1,2020-02-18 14:20:30,2020-02-18 14:46:30,20",
            "1,2020-02-18 14:47:20,2020-02-18 15:20:30,30",
            "1,2020-02-18 15:37:23,2020-02-18 16:05:26,40",
            "1,2020-02-18 16:06:27,2020-02-18 17:20:49,50",
            "1,2020-02-18 17:21:50,2020-02-18 18:03:27,60",
            "2,2020-02-18 14:18:24,2020-02-18 15:01:40,20",
            "2,2020-02-18 15:20:49,2020-02-18 15:30:24,30",
            "2,2020-02-18 16:01:23,2020-02-18 16:40:32,40",
            "2,2020-02-18 16:44:56,2020-02-18 17:40:52,50",
            "3,2020-02-18 14:39:58,2020-02-18 15:35:53,20",
            "3,2020-02-18 15:36:39,2020-02-18 15:24:54,30"
        )).map(line => {
            val arr = line.split(",")
            Action(arr(0), arr(1), arr(2),arr(3).toInt)
        })

        // 对rdd分组
        rdd.groupBy(_.uid)
            .map(tp => {
                val uid = tp._1
                val actions = tp._2.toList.sortBy(_.startTime)
                var segments = new ListBuffer[ListBuffer[Action]]()
                var segment = new ListBuffer[Action]()
                for(i <- actions.indices){
                    if(i != actions.size -1 && (actions(i+1).startTime - actions(i).endTime) < 10*60*60){ // 是否超出间隔
                        segment += actions(i)
                    }else{
                        segment += actions(i)
                        segments += segment
                        segment = new ListBuffer[Action]()
                    }
                }

                segments.map(lst => {
                    // 段落的起始时间,段落的结束时间, 段落的num累计和
                    (uid, lst.head.startTime,lst.last.endTime,lst.map(_.num).sum)
                })
            })
            .collect
            .foreach(println)


        sc.stop()
    }

    implicit class RichString(dt1:String){
        def -(dt2:String): Long ={
            val d1 = DateUtils.parseDate(dt1,"yyyy-MM-dd HH:mm:ss")
            val d2 = DateUtils.parseDate(dt2,"yyyy-MM-dd HH:mm:ss")
            d2.getTime - d1.getTime
        }
    }

}

