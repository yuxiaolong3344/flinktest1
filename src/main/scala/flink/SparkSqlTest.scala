package flink

import org.apache.spark.SparkConf
import org.apache.spark.sql._


object SparkSqlTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparksql").setMaster("local[*]")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = session.read.json("D:\\IdeaProjects\\flinktest1\\People.json")
    //df.show()
    df.createOrReplaceTempView("pl")
    //println("---------------------------")
   df.printSchema()
    df.select("name").show()

  }
}

case class People(age:BigInt,name:String)

