package cn.doitedu.spark.util

import cn.doitedu.spark.demos.IpRule

import scala.annotation.tailrec

/**
 * @author malichun
 * @create 2022/12/06 0006 13:53
 */
object ExerciseUtil {

    /**
     * 二分搜索
     * @param ipAreaDict
     * @param ipInt
     * @return
     */
    def binarySearch(ipAreaDict:List[IpRule], ipInt:Long):Int = {
        binarySearch(ipAreaDict, 0, ipAreaDict.length-1, ipInt)
    }

    @tailrec
    private def binarySearch(ipAreaDict:List[IpRule], low:Int, high:Int, ipInt:Long):Int={
        val midIndex = (low + high)/2
        val midIpAreaDict = ipAreaDict(midIndex)
        if(midIpAreaDict.startIp <= ipInt && midIpAreaDict.endIp >= ipInt){
            return midIndex
        }
        if(low >= high){
            return -1;
        }
        if(midIpAreaDict.startIp < ipInt){
            binarySearch(ipAreaDict,midIndex+1, high,ipInt)
        }else{
            binarySearch(ipAreaDict, low, midIndex, ipInt)
        }
    }

    def main(args: Array[String]): Unit = {

        val arr = List(
            IpRule(10,100,"a","a","a","a","a"),
            IpRule(110,200,"b","b","b","b","b"),
            IpRule(220,350,"c","c","b","b","b")
        )

        val idx: Int = binarySearch(arr, 220)

        println(idx)



    }
}
