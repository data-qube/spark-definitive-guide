package com.cffex.ch05

import org.apache.spark.sql.SparkSession

object Basic_Structured_Operations{

  def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("ch05")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // in Scala
    //spark.read.format("json").load("data/flight-data/json/2015-summary.json").schema
    // COMMAND ----------
    // in Scala
    import org.apache.spark.sql.types._
    val myManualSchema1 = StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      StructField("count", LongType, false,
        Metadata.fromJson("{\"hello\":\"world\"}"))
    ))

    val df1 = spark.read.format("json").schema(myManualSchema1)
      .load("data/flight-data/json/2015-summary.json")

    // COMMAND ----------
    // in Scala
    import org.apache.spark.sql.functions.{col, column}
    col("someColumnName")
    column("someColumnName")
    // COMMAND ----------
    // in Scala
//    $"myColumn"
//    'myColumn

    // COMMAND ----------
    df1.col("count")
    // COMMAND ----------
    (((col("someCol") + 5) * 200) - 6) < col("otherCol")
    // COMMAND ----------
    // in Scala
    import org.apache.spark.sql.functions.expr
    expr("(((someCol + 5) * 200) - 6) < otherCol")

    // COMMAND ----------
    spark.read.format("json").load("data/flight-data/json/2015-summary.json").columns
    // COMMAND ----------
    df1.first()
    // COMMAND ----------
    // in Scala
    import org.apache.spark.sql.Row
    val myRow = Row("Hello", null, 1, false)
    // COMMAND ----------
    // in Scala
    myRow(0) // type Any
    myRow(0).asInstanceOf[String] // String
    myRow.getString(0) // String
    myRow.getInt(2) // Int

    // COMMAND ----------
    // in Scala
    val df = spark.read.format("json")
      .load("data/flight-data/json/2015-summary.json")
    df.createOrReplaceTempView("dfTable")

    // COMMAND ----------
    // in Scala
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

    val myManualSchema = new StructType(Array(
      new StructField("some", StringType, true),
      new StructField("col", StringType, true),
      new StructField("names", LongType, false)))
    val myRows = Seq(Row("Hello", null, 1L))
    val myRDD = spark.sparkContext.parallelize(myRows)
    val myDf = spark.createDataFrame(myRDD, myManualSchema)
    myDf.show()

    // COMMAND ----------
    // in Scala
    val myDF = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3")

    // COMMAND ----------
    // in Scala
    df.select("DEST_COUNTRY_NAME").show(2)

    // COMMAND ----------
    // in Scala
    df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)
    // COMMAND ----------
    // in Scala
    import org.apache.spark.sql.functions.{col, column, expr}

    df.select(
      df.col("DEST_COUNTRY_NAME"),
      col("DEST_COUNTRY_NAME"),
      column("DEST_COUNTRY_NAME"),
      'DEST_COUNTRY_NAME,
      $"DEST_COUNTRY_NAME",
      expr("DEST_COUNTRY_NAME"))
      .show(2)
    // COMMAND ----------
    // in Scala
    df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
    // COMMAND ----------
    // in Scala
    df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME"))
      .show(2)
    // in Scala
    df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)
    // in Scala
    df.selectExpr(
      "*", // include all original columns
      "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
      .show(2)
    // in Scala
    df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
    // in Scala
    import org.apache.spark.sql.functions.lit
    df.select(expr("*"), lit(1).as("One")).show(2)
    // in Scala
    df.withColumn("numberOne", lit(1)).show(2)
    // in Scala
    df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
      .show(2)
    df.withColumn("Destination", expr("DEST_COUNTRY_NAME")).columns
    // in Scala
    df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns
    // in Scala
    import org.apache.spark.sql.functions.expr

    val dfWithLongColName = df.withColumn(
      "This Long Column-Name",
      expr("ORIGIN_COUNTRY_NAME"))

    // in Scala
    dfWithLongColName.selectExpr(
      "`This Long Column-Name`",
      "`This Long Column-Name` as `new col`")
      .show(2)

    dfWithLongColName.createOrReplaceTempView("dfTableLong")
    // in Scala
    dfWithLongColName.select(col("This Long Column-Name")).columns

    df.drop("ORIGIN_COUNTRY_NAME").columns

    dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")

    df.withColumn("count2", col("count").cast("long"))

    df.filter(col("count") < 2).show(2)
    df.where("count < 2").show(2)
    // in Scala
    df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")
      .show(2)
    // in Scala
    df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()

    // in Scala
    df.select("ORIGIN_COUNTRY_NAME").distinct().count()

    val seed = 5
    val withReplacement = false
    val fraction = 0.5
    df.sample(withReplacement, fraction, seed).count()
    // in Scala
    val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)
    dataFrames(0).count() > dataFrames(1).count() // False
    // in Scala
    import org.apache.spark.sql.Row
    val schema = df.schema
    val newRows = Seq(
      Row("New Country", "Other Country", 5L),
      Row("New Country 2", "Other Country 3", 1L)
    )
    val parallelizedRows = spark.sparkContext.parallelize(newRows)
    val newDF = spark.createDataFrame(parallelizedRows, schema)
    df.union(newDF)
      .where("count = 1")
      .where($"ORIGIN_COUNTRY_NAME" =!= "United States")
      .show() // get all of them and we'll see our new rows at the end

    // in Scala
    df.sort("count").show(5)
    df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
    df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)
    // in Scala
    import org.apache.spark.sql.functions.{asc, desc}
    df.orderBy(expr("count desc")).show(2)
    df.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(2)
    // in Scala
    spark.read.format("json").load("data/flight-data/json/*-summary.json")
      .sortWithinPartitions("count")
    // in Scala
    df.limit(5).show()
    // in Scala
    df.orderBy(expr("count desc")).limit(6).show()
    // in Scala
    df.rdd.getNumPartitions // 1
    // in Scala
    df.repartition(5)
    // in Scala
    df.repartition(col("DEST_COUNTRY_NAME"))
    // in Scala
    df.repartition(5, col("DEST_COUNTRY_NAME"))
    // in Scala
    df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)
    // COMMAND ----------
    val collectDF = df.limit(10)
    collectDF.take(5) // take works with an Integer count
    collectDF.show() // this prints it out nicely
    collectDF.show(5, false)
    collectDF.collect()
    // COMMAND ----------
    collectDF.toLocalIterator()
  }
}