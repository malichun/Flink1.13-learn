package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * 统计连续登陆的天数>=3的用户，以及发生的起止日期
 *
 * - 这个问题可以扩展到很多相似的问题：连续几个月充值会员、连续天数有商品卖出、连续打滴滴、连续逾期。
 *
 * - 测试数据：用户ID、登入日期
 *
 * guid01,2018-02-28
 * guid01,2018-03-01
 * guid01,2018-03-02
 * guid01,2018-03-04
 * guid01,2018-03-05
 * guid01,2018-03-06
 * guid01,2018-03-07
 * guid02,2018-03-01
 * guid02,2018-03-02
 * guid02,2018-03-03
 * guid02,2018-03-06
 *
 *
 * 期望得到的结果：
 * guid01,4,2018-03-04,2018-03-07
 * guid02,3,2018-03-01,2018-03-03
 *
 * sql写一遍：
 * with tmp as (
 * select
 * guid,
 * dt,
 * row_number() over() as rn
 * from t
 * )
 * select
 * guid,
 * min(dt) as start_dt,
 * max(dt) as end_dt,
 * count(1) as days
 * from tmp
 * group by guid,date_sub(dt,rn)
 *
 */
object X06_综合练习_6_统计连续登录的天数 {
    def main(args: Array[String]): Unit = {
        val sc = SparkContextUtil.getSc("月累计金额计算")

        val rdd = sc.parallelize(Seq(
            "guid01,2018-02-28",
            "guid01,2018-03-01",
            "guid01,2018-03-02",
            "guid01,2018-03-05",
            "guid01,2018-03-04",
            "guid01,2018-03-06",
            "guid01,2018-03-07",
            "guid02,2018-03-01",
            "guid02,2018-03-02",
            "guid02,2018-03-03",
            "guid02,2018-03-06"
        ))

        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        rdd.map(line => {
            val arr = line.split(",")
            (arr(0), arr(1))
        })
            .distinct
            .groupByKey()
            .flatMap { case (person, days) =>
                days
                    .toList
                    .sorted
                    .zipWithIndex
                    .map { case (day, diffDays) =>
                        val calStartDay = sdf.format(new Date(sdf.parse(day).getTime - diffDays * 3600 * 24))
                        // 计算的天, 真实的天
                        (calStartDay, day)
                    }
                    .foldLeft(ListBuffer[(String, String, String, Int)](("","","",0))) { case (listBuffer, (calStartDay, day)) =>
                        var before = listBuffer.last // (计算相等的日期, 开始时间, 结束时间, 次数)
                        if (before._1==calStartDay) {
                            if(before._2 != "") {
                                before = (calStartDay, before._2, day, before._4 + 1)
                            }else{
                                before = (calStartDay, day, day, before._4 + 1)
                            }
                        }else{
                            before = (calStartDay, day, day , 1)
                        }
                        listBuffer.update(listBuffer.length-1, before)
                        listBuffer
                    }
                .filter(_._4>=3)
                .map(t => (person, t._2, t._3, t._4))

            }
            .collect()
            .foreach(println(_))

        sc.stop()
    }
}
