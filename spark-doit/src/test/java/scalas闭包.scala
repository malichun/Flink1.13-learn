/**
 * @author malichun
 * @create 2022/11/28 0028 22:41
 */
object scalas闭包 {
    def main(args: Array[String]): Unit = {
        var a = 100

        val f = (i:Int) => {
            val res = 10 + i + a
            a = 500 // 里面修改外面的a
            res
        }// 闭包
        println(f(10)) // 120
        println(a) // 500, a会变

        a = 200
        println(f(10)) // 220
    }
}
