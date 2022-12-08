package org.apache.spark

import org.apache.spark.rdd.RDD

/**
 * 做了一个马甲
 */
object CheckPointLoader {

    def load(sc:SparkContext, path:String): RDD[(String, Int)] ={
        sc.checkpointFile[(String,Int)](path)
    }

}
