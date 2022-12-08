package cn.doitedu.spark.deepin

import java.io.{BufferedReader, FileReader}
import scala.collection.mutable

/**
 * 文件的迭代器
 * 文件路径
 */
class MyFileIterator(filePath:String) extends Iterator[String]{ // 独处数据的泛型

    private val br = new BufferedReader(new FileReader(filePath))
    var line:String = _

    override def hasNext: Boolean = {
        line = br.readLine()
        if(line == null){
            br.close()
        }
        line!=null
    }

    override def next(): String = {
        line
    }
}

/**
 * iterable
 * 对iterator再封装
 * @param filePath
 */
class MyFileIterable(filePath:String) extends Iterable[String]{
    override def iterator: Iterator[String] = new MyFileIterator(filePath)
}

object MyFileIteratorTest{
    def main(args: Array[String]): Unit = {
        // 迭代器不一定挂在集合上面的
        val iterable: Iterable[String] = new MyFileIterable("data/wordcount/input/a.txt")

//        val iter: Iterator[String] = iterable.iterator
//        while(iter.hasNext){
//            println(iter.next())
//        }

        println("----------------------分隔线-------------------------")

        // wordcount
        iterable.flatMap(s => s.split("\\s+"))
            .map(s => (s,1))
            .groupBy(_._1)
            .map(t => {
                (t._1,t._2.map(_._2).sum)
            })
            .foreach(println)

    }
}
