package cn.doitedu.chapter02_baseapi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author malc
 * @create 2022/7/29 0029 11:51
 */
object WindowWordCount2 {
    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.socketTextStream("hadoop01",6666)
            .flatMap(_.toLowerCase.split("\\W+"))
            .filter(_ nonEmpty)
            .map((_,1))
            .keyBy(_._1)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .sum(1)
            .print()


        env.execute("Window Stream WordCount")

    }

}
