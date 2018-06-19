import org.apache.spark.sql.SparkSession

object Monitoring_and_Debugging{
  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder
      .master("mesos://HOST:5050")
      .appName("my app")
      .config("spark.executor.uri", "<path to spark-2.2.0.tar.gz uploaded above>")
      .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    // COMMAND ----------
  }
}