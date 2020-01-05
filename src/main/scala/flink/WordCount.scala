package flink

import org.apache.flink.api.scala._


object WordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val inputpath = "D:\\IdeaProjects\\flinktest1\\in.txt"
    val DS: DataSet[String] = env.readTextFile(inputpath)
    val resultSet: AggregateDataSet[(String, Int)] = DS.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
    resultSet.print()

  }

}
