package cn.doitedu.spark.deepin

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.lib.CombineTextInputFormat

/**
 * @author malc
 * @create 2022/12/9 0009 18:43
 */
object D_01源头RDD分区数原理_小文件效率低问题 {
    def main(args: Array[String]): Unit = {
        val sc = SparkContextUtil.getSc("D_01源头RDD分区数原理_小文件效率低问题")

        // sc.textFile底层就是调用的sc.hadoopFile, 但是InputFormat写死成TextInputFormat
        // 我们可以自己调用sc.hadoopFile, 传入自己选择的InputFormat(CombineTextInputFormat)来解决大量小文件问题
        val rdd = sc.textFile("path")

        // 使用 CombineTextInputFormat
        val rddCombine = sc.hadoopFile("path",classOf[CombineTextInputFormat],classOf[LongWritable],classOf[Text],2)


//        sc.parallelize()
    }

}
