package com.cffex.ch13

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Advanced_RDDs{
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)

    val config = new SparkConf().setAppName("FriendsByAge")
    val sc = new SparkContext(config);
    // in Scala
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    val words = sc.parallelize(myCollection, 2)
    // in Scala
    words.map(word => (word.toLowerCase, 1))
    // in Scala
    val keyword = words.keyBy(word => word.toLowerCase.toSeq(0).toString)
    // in Scala
    keyword.mapValues(word => word.toUpperCase).collect()
    // in Scala
    keyword.flatMapValues(word => word.toUpperCase).collect()
    // in Scala
    keyword.keys.collect()
    keyword.values.collect()
    // COMMAND ----------
    keyword.lookup("s")
    // in Scala
    val distinctChars1 = words.flatMap(word => word.toLowerCase.toSeq).distinct
      .collect()
    import scala.util.Random
    val sampleMap = distinctChars1.map(c => (c, new Random().nextDouble())).toMap
    words.map(word => (word.toLowerCase.toSeq(0), word))
      .sampleByKey(true, sampleMap, 6L)
      .collect()
    // in Scala
    words.map(word => (word.toLowerCase.toSeq(0), word))
      .sampleByKeyExact(true, sampleMap, 6L).collect()
    // in Scala
    val chars = words.flatMap(word => word.toLowerCase.toSeq)
    val KVcharacters = chars.map(letter => (letter, 1))
    def maxFunc(left:Int, right:Int) = math.max(left, right)
    def addFunc(left:Int, right:Int) = left + right
    val nums = sc.parallelize(1 to 30, 5)
    // in Scala
    val timeout = 1000L //milliseconds
    val confidence = 0.95
    KVcharacters.countByKey()
    KVcharacters.countByKeyApprox(timeout, confidence)
    // in Scala
    KVcharacters.groupByKey().map(row => (row._1, row._2.reduce(addFunc))).collect()
    // COMMAND ----------
    KVcharacters.reduceByKey(addFunc).collect()
    // COMMAND ----------
    // in Scala
    nums.aggregate(0)(maxFunc, addFunc)
    // in Scala
    val depth = 3
    nums.treeAggregate(0)(maxFunc, addFunc, depth)
    // COMMAND ----------
    KVcharacters.aggregateByKey(0)(addFunc, maxFunc).collect()
    // in Scala
    val valToCombiner = (value:Int) => List(value)
    val mergeValuesFunc = (vals:List[Int], valToAppend:Int) => valToAppend :: vals
    val mergeCombinerFunc = (vals1:List[Int], vals2:List[Int]) => vals1 ::: vals2
    // now we define these as function variables
    val outputPartitions1 = 6
    KVcharacters
      .combineByKey(
        valToCombiner,
        mergeValuesFunc,
        mergeCombinerFunc,
        outputPartitions1)
      .collect()
    // in Scala
    KVcharacters.foldByKey(0)(addFunc).collect()
    // in Scala
    import scala.util.Random
    val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
    val charRDD = distinctChars.map(c => (c, new Random().nextDouble()))
    val charRDD2 = distinctChars.map(c => (c, new Random().nextDouble()))
    val charRDD3 = distinctChars.map(c => (c, new Random().nextDouble()))
    charRDD.cogroup(charRDD2, charRDD3).take(5)
    // in Scala
    val keyedChars = distinctChars.map(c => (c, new Random().nextDouble()))
    val outputPartitions = 10
    KVcharacters.join(keyedChars).count()
    KVcharacters.join(keyedChars, outputPartitions).count()
    // in Scala
    val numRange = sc.parallelize(0 to 9, 2)
    words.zip(numRange).collect()
    // in Scala
    words.coalesce(1).getNumPartitions // 1
    words.repartition(10) // gives us 10 partitions
    // in Scala
    val df = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("/data/retail-data/all/")
    val rdd = df.coalesce(10).rdd
    // COMMAND ----------
    df.printSchema()
    // in Scala
    import org.apache.spark.HashPartitioner
    rdd.map(r => r(6)).take(5).foreach(println)
    val keyedRDD = rdd.keyBy(row => row(6).asInstanceOf[Int].toDouble)
    keyedRDD.partitionBy(new HashPartitioner(10)).take(10)
    // COMMAND ----------
    // in Scala
    import org.apache.spark.Partitioner
    class DomainPartitioner extends Partitioner {
      def numPartitions = 3
      def getPartition(key: Any): Int = {
        val customerId = key.asInstanceOf[Double].toInt
        if (customerId == 17850.0 || customerId == 12583.0) {
          return 0
        } else {
          return new java.util.Random().nextInt(2) + 1
        }
      }
    }

    keyedRDD
      .partitionBy(new DomainPartitioner).map(_._1).glom().map(_.toSet.toSeq.length)
      .take(5)


    // COMMAND ----------

    // in Scala
    class SomeClass extends Serializable {
      var someValue = 0
      def setSomeValue(i:Int) = {
        someValue = i
        this
      }
    }
//    sc.parallelize(1 to 10).map(num => new SomeClass().setSomeValue(num))
//    // in Scala
//    val conf = new SparkConf().setMaster(...).setAppName(...)
//    conf.registerKryoClasses(Array(classOf[MyClass1], classOf[MyClass2]))
//    val sc = new SparkContext(conf)
    // COMMAND ----------
  }
}



