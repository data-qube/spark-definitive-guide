package com.cffex.ch14

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Distributed_Variables{
  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()


    // in Scala
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 2)
    // in Scala
    val supplementalData = Map("Spark" -> 1000, "Definitive" -> 200,
      "Big" -> -300, "Simple" -> 100)
    // in Scala
    val suppBroadcast = spark.sparkContext.broadcast(supplementalData)
    // in Scala
    suppBroadcast.value
    // in Scala
    words.map(word => (word, suppBroadcast.value.getOrElse(word, 0)))
      .sortBy(wordPair => wordPair._2)
      .collect()
    // in Scala
    case class Flight(DEST_COUNTRY_NAME: String,
                      ORIGIN_COUNTRY_NAME: String, count: BigInt)
    val flights = spark.read
      .parquet("/data/flight-data/parquet/2010-summary.parquet")
      .as[Flight]
    // in Scala
    import org.apache.spark.util.LongAccumulator
    val accUnnamed = new LongAccumulator
    val acc = spark.sparkContext.register(accUnnamed)
    // in Scala
    val accChina = new LongAccumulator
    val accChina2 = spark.sparkContext.longAccumulator("China")
    spark.sparkContext.register(accChina, "China")
    // in Scala
    def accChinaFunc(flight_row: Flight) = {
      val destination = flight_row.DEST_COUNTRY_NAME
      val origin = flight_row.ORIGIN_COUNTRY_NAME
      if (destination == "China") {
        accChina.add(flight_row.count.toLong)
      }
      if (origin == "China") {
        accChina.add(flight_row.count.toLong)
      }
    }
    // in Scala
    flights.foreach(flight_row => accChinaFunc(flight_row))
    // in Scala
    accChina.value // 953
    // in Scala
    import org.apache.spark.util.AccumulatorV2

    import scala.collection.mutable.ArrayBuffer

    val arr = ArrayBuffer[BigInt]()

    class EvenAccumulator extends AccumulatorV2[BigInt, BigInt] {
      private var num:BigInt = 0
      def reset(): Unit = {
        this.num = 0
      }
      def add(intValue: BigInt): Unit = {
        if (intValue % 2 == 0) {
          this.num += intValue
        }
      }
      def merge(other: AccumulatorV2[BigInt,BigInt]): Unit = {
        this.num += other.value
      }
      def value():BigInt = {
        this.num
      }
      def copy(): AccumulatorV2[BigInt,BigInt] = {
        new EvenAccumulator
      }
      def isZero():Boolean = {
        this.num == 0
      }
    }
//    val acc = new EvenAccumulator
//    val newAcc = sc.register(acc, "evenAcc")
//    // COMMAND ----------
//    acc.value // 0
//    flights.foreach(flight_row => acc.add(flight_row.count))
//    acc.value // 31390
    // COMMAND ----------
  }
}