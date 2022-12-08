package cn.doitedu.spark.util

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author malichun
 * @create 2022/11/27 0027 23:30
 */
object SparkContextUtil {

    def getSc(appName:String, master:String="local"):SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)
        conf.setMaster(master)
        new SparkContext(conf)
    }

}
