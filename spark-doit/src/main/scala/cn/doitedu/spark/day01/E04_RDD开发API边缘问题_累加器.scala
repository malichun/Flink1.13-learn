package cn.doitedu.spark.day01

import cn.doitedu.spark.util.SparkContextUtil
import com.alibaba.fastjson.JSON

/**
 *
 * 累加器, 主要用于正常业务过程中的一些附属信息统计
 *  (
 *      比如程序遇到的脏数据条数,
 *      程序处理的数据总行数,
 *      程序处理的特定数据条数,
 *      程序处理所花费的时长
 *  )
 *
 * 解析不正常有多少次
 * 求每种性别的平均年龄
 *
{"id":1,"sex":"male","name":"aaa","age":18}
{"id":2,"sex":"male","name":"bbb","age":}
{"id":3,"sex":"male","name":"ccc","age":38
{"id":4,"sex":"female","name":"ddd","age":16}
{"id":5,"sex":"female","name":eee","age":28}
{"id":6,"sex":"female","name":"fff","age":16}
 */
object E04_RDD开发API边缘问题_累加器 {
    def main(args: Array[String]): Unit = {
        val sc = SparkContextUtil.getSc("累加器")

        // 解析异常的累加器
        val accumulator = sc.longAccumulator("malformedJsonLines")
        val calcTimeLongAccumulator = sc.longAccumulator("calcTimeLongAccumulator")

        val jsonRDD = sc.textFile("data/json/input/a.txt")

        // 解析json
        val rdd2 = jsonRDD.map(json => {
            val start = System.currentTimeMillis()
            var res:(String, Integer) = null;
            try {
                val jsonObject = JSON.parseObject(json)
                val id = jsonObject.getInteger("id")
                val name = jsonObject.getString("name")
                val sex = jsonObject.getString("sex")
                val age = jsonObject.getInteger("age")
                res = (sex, age)
            }catch {
                case e:Exception =>
                    accumulator.add(1L)
            }
            val end = System.currentTimeMillis()
            calcTimeLongAccumulator.add(end-start)
            res
        })

        rdd2.foreach(println)

//        val nullsRDD = rdd2.filter(tp => tp == null)
//        println(nullsRDD.count())

        // 各个executor的结果汇报给driver, driver端做汇总
        println(s"不合法的json行数为: ${accumulator.value}")


    }
}
