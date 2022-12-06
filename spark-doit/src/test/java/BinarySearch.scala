import java.util.Collections
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * @author malichun
 * @create 2022/12/02 0002 1:12
 */
object BinarySearch {
    def main(args: Array[String]): Unit = {
        val lst = List(1,2,3,5,6,8,10,13,16,17)
        lst.zipWithIndex.foreach(println)
        println(binarySearch(lst, 0, lst.length-1, 1))


    }

   def binarySearch(lst:List[Int], start:Int, end:Int, search:Int): Int ={

       val mIndex = (start+end)/2
       val mid = lst(mIndex)

       if(mid == search){
           return mIndex
       }
       if(start >= end){
           return -1
       }
       if( mid >search){
           binarySearch(lst, start,mIndex,search)
       }else {
           binarySearch(lst,mIndex+1, end, search)
       }
   }
}
