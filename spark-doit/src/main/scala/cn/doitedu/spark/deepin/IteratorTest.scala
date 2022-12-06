package cn.doitedu.spark.deepin

/**
 * 迭代器
 */
object IteratorTest {
    def main(args: Array[String]): Unit = {
        val lst = List(1, 2, 3, 4, 5)
//        lst.map(e => println(e)) // list直接map会打印

        /////////////////////////////////////////////////

        /**
         * scala迭代算子lazy特性
         * 相当于rdd上的transformation算子
         */
        val iter1 = lst.iterator

        val iter2 = iter1.map(e => {
            println("映射1函数被执行了...")
            e + 10
        })  // 不会执行

        val iter3 = iter2.map(e => {
            println("函数2被执行了....")
            e * 100
        }) // 不会执行

        // iter3
        // hasNext = iter2.hasNext => iter1.hasNext
        // next()  = f2(iter2.next(f1(iter.next)))

        // 相当于RDD中的action算子
        iter3.foreach(println)


    }

}
