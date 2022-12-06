package cn.doitedu.spark.deepin

import java.sql.{DriverManager, ResultSet}

case class Solider(id:Int, name:String, role:String, battle:Double)

class MySqlTableIterator(url:String, username:String, password:String, tableName:String) extends Iterator[Solider]{
    Class.forName("com.mysql.cj.jdbc.Driver")
    private val conn = DriverManager.getConnection(url, username, password)
    private val stmt = conn.createStatement()
    private val resultSet: ResultSet = stmt.executeQuery(s"select * from ${tableName}")

    override def hasNext: Boolean = {
        val next = resultSet.next()
        if(!next){
            stmt.close()
            conn.close()
        }
        next
    }

    override def next(): Solider = {
        val id = resultSet.getInt("id")
        val name = resultSet.getString("name")
        val role = resultSet.getString("role")
        val battle = resultSet.getDouble("battle")
        Solider(id, name, role, battle)
    }
}

class MySqlTableIterable(url:String, username:String, password:String, tableName:String) extends Iterable[Solider]{
    override def iterator: Iterator[Solider] = new MySqlTableIterator(url, username, password, tableName)
}

object MySqlTableIteratorTest{
    def main(args: Array[String]): Unit = {
        val iter = new MySqlTableIterable("jdbc:mysql://localhost:3306", "root", "123456", "abc.battle")

        val res = iter.map(solider => (solider.role,solider.battle))
            .groupBy(_._1)
            .mapValues(iter => iter.map(_._2).sum)

        res.foreach(println)

    }
}