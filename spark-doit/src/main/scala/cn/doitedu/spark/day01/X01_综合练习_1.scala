package cn.doitedu.spark.day01

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 对sanguo.txt
 * 和heros.txt中的数据, 做如下统计
 *
 * 每个城市的杀人总数, 关押总数
 * 每个城市的杀人最多的将军信息
 */
object X01_综合练习_1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("联系")
        conf.setMaster("local[2]")
        val sc = new SparkContext(conf)
//        val battleRecord = sc.textFile("/tmp/sanguo.txt")
//      val general = sc.textFile("/tmp/sanguo.txt")

        // 加载战斗记录数据 2,kill,6
        val battleRecord = sc.textFile("data/sanguo/input")
        val battleRecord2 = battleRecord.map(line => {
            val arr = line.split(",")
            (arr(0).toInt, (arr(1), arr(2).toInt))
        }).cache()

        // 加载将军信息数据  {"id":5,"name":"虚竹","age":23,"city":"深圳"}
        val general = sc.textFile("data/sanguo/heros")
        val general2 = general.map(line => {
            val jsonObject = JSON.parseObject(line)
            val id = jsonObject.getIntValue("id")
            val name = jsonObject.getString("name")
            val age = jsonObject.getString("age")
            val city = jsonObject.getString("city")
            (id, (name, age, city))
        })

        val cityRDD = general2.join(battleRecord2)
            .map { case (user_id, ((name, age, city), (battleType, num))) =>
                ((city, battleType), num)
            }
            .reduceByKey(_ + _)
            .map { case ((city, battleType), num) =>
                (city, (battleType, num))
            }
            .aggregateByKey((0, 0))((u, e) => {
                val killNumAndGuanyaNumTuple = e._1 match {
                    case "kill" => (e._2, 0)
                    case "guanya" => (0, e._2)
                    case _ => (0, 0)
                }
                (u._1 + killNumAndGuanyaNumTuple._1, u._2 + killNumAndGuanyaNumTuple._2)
            }, (u1, u2) => {
                (u1._1 + u2._1, u1._2 + u2._2)
            })

        // 每个城市的杀人总数, 关押总数
        cityRDD.map(t => (t._1, t._2._1, t._2._2)).foreach(println)

        println("================================")
        //每个城市的杀人最多的将军信息
        battleRecord2
            .filter(t => t._2._1 == "kill")
            .map(t => (t._1, t._2._2))
            .reduceByKey(_ + _)
            .join(general2)
            .map { case (userId, (killNum, (name, age, city))) =>
                (city, (userId, name, age, city, killNum))
            }
            .reduceByKey((t1, t2) => {
                if (t1._5 > t2._5) {
                    t1
                } else {
                    t2
                }
            }).foreach(println(_))



            sc.stop()
    }
}
