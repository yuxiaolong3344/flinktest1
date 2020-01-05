package flink

import org.apache.flink.streaming.api.scala._

object WC {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[String] = env.socketTextStream("hadoop102",7777)
    val rsds: DataStream[(String, Int)] = ds.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(0).sum(1)
    rsds.print().setParallelism(1)
    env.execute()


  }
}
