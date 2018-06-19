import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Spark_Runs_on_a_Cluster{
  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)
//    val spark = SparkSession.builder()
//      .appName("test")
//      .master("local[*]")
//      .getOrCreate()
    // Creating a SparkSession in Scala
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().appName("Databricks Spark Example")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()
    // COMMAND ----------
    import org.apache.spark.SparkContext
    val sc = SparkContext.getOrCreate()
    // COMMAND ----------
//    step4.explain()

    spark.conf.set("spark.sql.shuffle.partitions", 50)
  }
}

