package cn.doitedu.spark.day01

import cn.doitedu.spark.util.{ExerciseUtil, SparkContextUtil}
import org.apache.spark.rdd.JdbcRDD

import java.io.{BufferedReader, File, FileReader}
import java.sql.{DriverManager, ResultSet}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
 * ip归属地
 */
case class IpRule(startIp:Long, endIp:Long, zhou:String, country:String, province:String, city:String, district:String)
object X04_综合练习_4_ip解析 {
    def main(args: Array[String]): Unit = {
        val sc = SparkContextUtil.getSc("IP地址归属地查询")

        // 累加器
        val dirty_cnt = sc.longAccumulator("dirty_cnt")

        // 加载ip归属地数据, 广播出去
        val ipSource = Source.fromFile("data/exersize/tm2/iprule/ip.txt")
        val ipList = ipSource.getLines().toList
        ipSource.close()
        val ipAreaInfo = ipList.map(line => {
            val arr = line.split("\\|")
            IpRule(arr(2).toLong, arr(3).toLong, arr(4), arr(5), arr(6), arr(7), arr(8))
        }).sortBy(_.startIp)
        ipAreaInfo.take(2).foreach(println)
        // 广播
        val ipAreaInfoBC = sc.broadcast(ipAreaInfo)


        // 加载用户访问日志
        //        val source = Source.fromFile("data/exersize/tm2/logdata/ipaccess.log","GBK")
        //        val logList = source.getLines().toList
        //        source.close()

        val br = new BufferedReader(new FileReader("data/exersize/tm2/logdata/ipaccess.log"))
        val list = ListBuffer[String]()
        var line: String = null
        @volatile var flag = true
        while (flag) {
            line = br.readLine()
            if (line == null) {
                flag = false
            }
            list.append(line)
        }
        br.close()
        val rdd = sc.makeRDD(list)
        //        rdd.take(3).foreach(println)

        // 抽取日志数据中的ip地址
        val ipRdd = rdd.map(s => {
            var rt: (String, String) = null
            try {
                val arr = s.split("\\|")
                rt = (arr(1), s)
            } catch {
                case e: Exception =>
                case _ => dirty_cnt.add(1)
            }
            rt
        })

        val filteredRDD = ipRdd.filter(_ != null)

        // 查询ip的归属地, 核心代码
        val result = filteredRDD.mapPartitions(iter => {

            // 13783487436,1767683400,亚洲,中国,上海市,上海市,闵行区
            // 29375843888,2980000000,亚洲,中国,上海市,上海市,长宁区
            val ipAreaDict = ipAreaInfoBC.value
            // 二分搜索
            iter.map(tp => {
                // 取出日志数据中的ip地址
                val ip = tp._1

                // 对分段字符串形式的IP地址:20.0.0.1,按照.号分割
                val ipSegs = ip.split("\\.").map(_.toLong)
                val ipInt = ipSegs(0)*256*256*256+ipSegs(1)*256*256+ipSegs(2)*256*256+ipSegs(3)
                // 将查询到的地理名称字段拼接到原来的日志数据后面,返回
                var (zhou,country,province,city,district) = ("","","","","")
                val idx= ExerciseUtil.binarySearch(ipAreaDict, ipInt)
                if(idx != -1){
                    val rule = ipAreaDict(idx)
                    zhou = rule.zhou
                    country = rule.country
                    province = rule.province
                    city = rule.city
                    district = rule.district
                }
                s"${tp._2}|${zhou}|${country}|${province}|${city}|$district"
            })
        })

        // 保存结果
        println("============")
        println(result.count)
        result.take(10).foreach(println)

        sc.stop()
    }

}
