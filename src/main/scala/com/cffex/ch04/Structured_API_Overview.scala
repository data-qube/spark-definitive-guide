import org.apache.spark.sql.SparkSession

object Structured_API_Overview {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ch04")
      .master("local[*]")
      .getOrCreate()
    // in Scala
    val df = spark.range(500).toDF("number")
    df.select(df.col("number") + 10)
    // COMMAND ----------
    // in Scala
    spark.range(2).toDF().collect()
    // COMMAND ----------
    import org.apache.spark.sql.types._
    val b = ByteType
    // COMMAND ----------
    val df1 = spark.read.format("json")
      .load("data/flight-data/json/2015-summary.json").toDF()

    // COMMAND ----------
    df.printSchema()

  }
}