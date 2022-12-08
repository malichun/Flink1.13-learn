package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil
import com.alibaba.fastjson.JSON
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
import scala.language.postfixOps

/**
 * 自定义累加器
 */
object E05_RDD开发API边缘问题_自定义累加器 {
    def main(args: Array[String]): Unit = {
        val sc = SparkContextUtil.getSc("累加器")

        val id_malformed = sc.longAccumulator("id_malformed")
        val fields_notenouth = sc.longAccumulator("fields_notenouth")

        // 构造一个自定义累加器
        val accumulator = new CustomAccumulator(mutable.HashMap[String, Long]())
        // 将自定义累加器想sparkContext注册
        sc.register(accumulator)

        val info = sc.makeRDD(Seq(
            "1,Mr.duan,18,beijing",
            "2,Mr.zhao,28,beijing",
            "b,Mr.liu,24,shanghai",
            "4,Mr.nai,22,shanghai",
            "a,Mr.liu,24",
            "a,Mr.ma"
        ))

        val tuples = info.map(line => {
            var res:(Int,String,Int, String) =null
            try{
            val arr = line.split(",")
            val id = arr(0).toInt
            val name = arr(1)
            val age = arr(2) toInt
            val city = arr(3)
            res = (id,name,age,city)
            }catch {
                case e:ArrayIndexOutOfBoundsException => {
                    fields_notenouth.add(1)
                    accumulator.add("array", 1)
                }
                case e:NumberFormatException => {
                    id_malformed.add(1)
                    accumulator.add("number",1)
                }
                case _ => accumulator.add("other",1)
            }
            res
        })

        val res = tuples.filter(tp => tp!=null)
            .groupBy(tp => tp._4)
            .map(tp => {
                (tp._1, tp._2.size)
            })

        res.foreach(println)

        // 查看累加器的数值
        println(id_malformed.value)

        println("==========================")
        println(accumulator.value)

        sc.stop()

    }
}


/**
 * IN泛型: 指的是在累加器上调用add()方法所传入的参数的类型
 * OUT泛型: 指的是在累加器掉value所返回的数据的类型
 * 注意: 累加器中用于存储值的数据结构, 必须是线程安全的:
 *      因为累加器对象在一个executor中是被多个task线程共享的
 *
 *      本例中的HashMap, 其实是不合适的!!!!!
 *      应该改成 java.util.concurrent.ConcurrentHashMap
 */
class CustomAccumulator(val valueMap:mutable.HashMap[String,Long]) extends AccumulatorV2[(String,Int),mutable.HashMap[String,Long]]{


    val vm =new java.util.concurrent.ConcurrentHashMap[String,Long]()
    // 判断累加器是否是初始状态
    override def isZero: Boolean = valueMap.isEmpty

    // 拷贝一个累加器对象的方法
    override def copy(): AccumulatorV2[(String,Int), mutable.HashMap[String,Long]] = {
        val newMap = mutable.HashMap[String, Long]()

        valueMap.synchronized{
            valueMap.foreach(kv => newMap.put(kv._1, kv._2))
        }
        new CustomAccumulator(newMap)
    }

    // 将累加器终止的方法
    override def reset(): Unit = this.valueMap.clear()

    // 对累加器更新时的方法
    override def add(v: (String,Int)): Unit = {
        val key: String = v._1
        val value:Int = v._2
        val oldValue:Long = valueMap.getOrElse(key,0)
        valueMap.put(v._1, oldValue + value)
    }

    // driver端对各个task所产生的累加器进行结果合并的逻辑
    override def merge(other: AccumulatorV2[(String,Int), mutable.HashMap[String,Long]]): Unit = {
        other.value.foreach(t => {
            val key:String = t._1
            val value:Long = t._2
            val oldValue:Long = valueMap.getOrElse(key,0)
            this.valueMap.put(key, oldValue + value)

        })

    }

    // 从累加器上取值
    override def value:mutable.HashMap[String,Long] = this.valueMap

}