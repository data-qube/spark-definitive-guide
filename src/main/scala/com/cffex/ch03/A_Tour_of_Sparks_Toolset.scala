package com.cffex.ch03

import org.apache.spark.sql.SparkSession

object A_Tour_of_Sparks_Toolset {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ch03")
      .master("local[*]")
      .getOrCreate()

    // in Scala
    case class Flight(DEST_COUNTRY_NAME: String,
                      ORIGIN_COUNTRY_NAME: String,
                      count: BigInt)

    val flightsDF = spark.read
      .parquet("data/flight-data/parquet/2010-summary.parquet/")

    import spark.implicits._

//    val flights = flightsDF.as[Flight]
//
//    flights
//      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
//      .map(flight_row => flight_row)
//      .take(5)
//
//    flights
//      .take(5)
//      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
//      .map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))

    // in Scala
    val staticDataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/retail-data/by-day/*.csv")

    staticDataFrame.createOrReplaceTempView("retail_data")
    val staticSchema = staticDataFrame.schema
    // in Scala
    import org.apache.spark.sql.functions.{window, column, desc, col}
    staticDataFrame
      .selectExpr(
        "CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day"))
      .sum("total_cost")
      .show(5)

    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val streamingDataFrame = spark.readStream
      .schema(staticSchema)
      .option("maxFilesPerTrigger", 1)
      .format("csv")
      .option("header", "true")
      .load("data/retail-data/by-day/*.csv")

    streamingDataFrame.isStreaming // returns true
    // in Scala
    val purchaseByCustomerPerHour = streamingDataFrame
      .selectExpr(
        "CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy($"CustomerId", window($"InvoiceDate", "1 day"))
      .sum("total_cost")

    purchaseByCustomerPerHour.writeStream
      .format("memory") // memory = store in-memory table
      .queryName("customer_purchases") // the name of the in-memory table
      .outputMode("complete") // complete = all the counts should be in the table
      .start()
    // in Scala
    spark.sql("""
      SELECT *
      FROM customer_purchases
      ORDER BY `sum(total_cost)` DESC
      """)
      .show(5)

    staticDataFrame.printSchema()
    // in Scala
    import org.apache.spark.sql.functions.date_format
    val preppedDataFrame = staticDataFrame
      .na.fill(0)
      .withColumn("day_of_week", date_format($"InvoiceDate", "EEEE"))
      .coalesce(5)
    // in Scala
    val trainDataFrame = preppedDataFrame
      .where("InvoiceDate < '2011-07-01'")
    val testDataFrame = preppedDataFrame
      .where("InvoiceDate >= '2011-07-01'")

    trainDataFrame.count()
    testDataFrame.count()
    // in Scala
    import org.apache.spark.ml.feature.StringIndexer
    val indexer = new StringIndexer()
      .setInputCol("day_of_week")
      .setOutputCol("day_of_week_index")
    // in Scala
    import org.apache.spark.ml.feature.OneHotEncoder
    val encoder = new OneHotEncoder()
      .setInputCol("day_of_week_index")
      .setOutputCol("day_of_week_encoded")
    // in Scala
    import org.apache.spark.ml.feature.VectorAssembler

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded"))
      .setOutputCol("features")
    // in Scala
    import org.apache.spark.ml.Pipeline

    val transformationPipeline = new Pipeline()
      .setStages(Array(indexer, encoder, vectorAssembler))
    // in Scala
    val fittedPipeline = transformationPipeline.fit(trainDataFrame)
    // in Scala
    val transformedTraining = fittedPipeline.transform(trainDataFrame)
    transformedTraining.cache()
    // in Scala
    import org.apache.spark.ml.clustering.KMeans
    val kmeans = new KMeans()
      .setK(20)
      .setSeed(1L)
    // in Scala
    val kmModel = kmeans.fit(transformedTraining)

    kmModel.computeCost(transformedTraining)
    // in Scala
    val transformedTest = fittedPipeline.transform(testDataFrame)

    kmModel.computeCost(transformedTest)
    // in Scala
    spark.sparkContext.parallelize(Seq(1, 2, 3)).toDF()
  }

}
