package com.cffex.ch06

import org.apache.spark.sql.SparkSession

object Working_with_Different_Types_of_Data {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ch06")
      .master("local[*]")
      .getOrCreate()

    // in Scala
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/retail-data/by-day/2010-12-01.csv")
    df.printSchema()
    df.createOrReplaceTempView("dfTable")

    import org.apache.spark.sql.functions.lit
    df.select(lit(5), lit("five"), lit(5.0))

    import org.apache.spark.sql.functions.col
    df.where(col("InvoiceNo").equalTo(536365))
      .select("InvoiceNo", "Description")
      .show(5, false)

    import org.apache.spark.sql.functions.col
    df.where(col("InvoiceNo") === 536365)
      .select("InvoiceNo", "Description")
      .show(5, false)

    df.where("InvoiceNo = 536365")
      .show(5, false)

    df.where("InvoiceNo <> 536365")
      .show(5, false)

    // in Scala
    val priceFilter1 = col("UnitPrice") > 600
    val descripFilter1 = col("Description").contains("POSTAGE")

    df.where(col("StockCode").isin("DOT"))
      .where(priceFilter1.or(descripFilter1))
      .show()

    // in Scala
    val DOTCodeFilter = col("StockCode") === "DOT"
    val priceFilter = col("UnitPrice") > 600
    val descripFilter = col("Description").contains("POSTAGE")
    df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
      .where("isExpensive")
      .select("unitPrice", "isExpensive").show(5)

    // in Scala
    import org.apache.spark.sql.functions.{col, expr, not}
    df.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
      .filter("isExpensive")
      .select("Description", "UnitPrice").show(5)
    df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
      .filter("isExpensive")
      .select("Description", "UnitPrice").show(5)
    // COMMAND ----------
    df.where(col("Description").eqNullSafe("hello")).show()
    // in Scala
    import org.apache.spark.sql.functions.{expr, pow}
    val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
    df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)
    // in Scala
    df.selectExpr(
      "CustomerId",
      "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)
    // in Scala
    import org.apache.spark.sql.functions.{bround, round}
    df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)
    // in Scala
    import org.apache.spark.sql.functions.lit
    df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)
    // in Scala
    import org.apache.spark.sql.functions.corr
    df.stat.corr("Quantity", "UnitPrice")
    df.select(corr("Quantity", "UnitPrice")).show()
    // in Scala
    df.describe().show()
    // in Scala
    // in Scala
    val colName = "UnitPrice"
    val quantileProbs = Array(0.5)
    val relError = 0.05
    df.stat.approxQuantile("UnitPrice", quantileProbs, relError) // 2.51
    // in Scala
    df.stat.crosstab("StockCode", "Quantity").show()
    // in Scala
    df.stat.freqItems(Seq("StockCode", "Quantity")).show()
    // in Scala
    import org.apache.spark.sql.functions.monotonically_increasing_id
    df.select(monotonically_increasing_id()).show(2)
    // in Scala
    import org.apache.spark.sql.functions.initcap
    df.select(initcap(col("Description"))).show(2, false)
    // in Scala
    import org.apache.spark.sql.functions.{lower, upper}
    df.select(col("Description"),
      lower(col("Description")),
      upper(lower(col("Description")))).show(2)
    // in Scala
    import org.apache.spark.sql.functions._
    df.select(
      ltrim(lit("    HELLO    ")).as("ltrim"),
      rtrim(lit("    HELLO    ")).as("rtrim"),
      trim(lit("    HELLO    ")).as("trim"),
      lpad(lit("HELLO"), 3, " ").as("lp"),
      rpad(lit("HELLO"), 10, " ").as("rp")).show(2)

    // in Scala
    import org.apache.spark.sql.functions.regexp_replace
    val simpleColors1 = Seq("black", "white", "red", "green", "blue")
    val regexString1 = simpleColors1.map(_.toUpperCase).mkString("|")
    // the | signifies `OR` in regular expression syntax
    df.select(
      regexp_replace(col("Description"), regexString1, "COLOR").alias("color_clean"),
      col("Description")).show(2)
    // in Scala
    import org.apache.spark.sql.functions.translate
    df.select(translate(col("Description"), "LEET", "1337"), col("Description"))
      .show(2)
    // in Scala
    import org.apache.spark.sql.functions.regexp_extract
    val regexString = simpleColors1.map(_.toUpperCase).mkString("(", "|", ")")
    // the | signifies OR in regular expression syntax
    df.select(
      regexp_extract(col("Description"), regexString, 1).alias("color_clean"),
      col("Description")).show(2)
    // in Scala
    val containsBlack = col("Description").contains("BLACK")
    val containsWhite = col("DESCRIPTION").contains("WHITE")
    df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
      .where("hasSimpleColor")
      .select("Description").show(3, false)
    // in Scala
    val simpleColors = Seq("black", "white", "red", "green", "blue")
    val selectedColumns = simpleColors.map(color => {
      col("Description").contains(color.toUpperCase).alias(s"is_$color")
    }):+expr("*") // could also append this value
    df.select(selectedColumns:_*).where(col("is_white").or(col("is_red")))
      .select("Description").show(3, false)
    df.printSchema()
    // in Scala
    import org.apache.spark.sql.functions.{current_date, current_timestamp}
    val dateDF = spark.range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())
    dateDF.createOrReplaceTempView("dateTable")

    dateDF.printSchema()
    // in Scala
    import org.apache.spark.sql.functions.{date_add, date_sub}
    dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)
    // in Scala
    import org.apache.spark.sql.functions.{datediff, months_between, to_date}
    dateDF.withColumn("week_ago", date_sub(col("today"), 7))
      .select(datediff(col("week_ago"), col("today"))).show(1)
    dateDF.select(
      to_date(lit("2016-01-01")).alias("start"),
      to_date(lit("2017-05-22")).alias("end"))
      .select(months_between(col("start"), col("end"))).show(1)
    // in Scala
    import org.apache.spark.sql.functions.{lit, to_date}
    spark.range(5).withColumn("date", lit("2017-01-01"))
      .select(to_date(col("date"))).show(1)
    // COMMAND ----------
    dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)
    // COMMAND ----------
    import org.apache.spark.sql.functions.to_date
    val dateFormat = "yyyy-dd-MM"
    val cleanDateDF = spark.range(1).select(
      to_date(lit("2017-12-11"), dateFormat).alias("date"),
      to_date(lit("2017-20-12"), dateFormat).alias("date2"))
    cleanDateDF.createOrReplaceTempView("dateTable2")
    // in Scala
    import org.apache.spark.sql.functions.to_timestamp
    cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()
    cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()
    cleanDateDF.filter(col("date2") > "'2017-12-12'").show()
    // in Scala
    import org.apache.spark.sql.functions.coalesce
    df.select(coalesce(col("Description"), col("CustomerId"))).show()

    df.na.drop()
    df.na.drop("any")
    df.na.drop("all")

    // in Scala
    df.na.drop("all", Seq("StockCode", "InvoiceNo"))
    // COMMAND ----------
    df.na.fill("All Null values become this string")
    // in Scala
    df.na.fill(5, Seq("StockCode", "InvoiceNo"))
    // in Scala
    val fillColValues = Map("StockCode" -> 5, "Description" -> "No Value")
    df.na.fill(fillColValues)
    // in Scala
    df.na.replace("Description", Map("" -> "UNKNOWN"))
    df.selectExpr("(Description, InvoiceNo) as complex", "*")
    df.selectExpr("struct(Description, InvoiceNo) as complex", "*")
    // in Scala
    import org.apache.spark.sql.functions.struct
    val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
    complexDF.createOrReplaceTempView("complexDF")
    complexDF.select("complex.Description")
    complexDF.select(col("complex").getField("Description"))
    complexDF.select("complex.*")
    // in Scala
    import org.apache.spark.sql.functions.split
    df.select(split(col("Description"), " ")).show(2)
    // in Scala
    df.select(split(col("Description"), " ").alias("array_col"))
      .selectExpr("array_col[0]").show(2)
    // in Scala
    import org.apache.spark.sql.functions.size
    df.select(size(split(col("Description"), " "))).show(2) // shows 5 and 3
    // in Scala
    import org.apache.spark.sql.functions.array_contains
    df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)
    // in Scala
    import org.apache.spark.sql.functions.{explode, split}

    df.withColumn("splitted", split(col("Description"), " "))
      .withColumn("exploded", explode(col("splitted")))
      .select("Description", "InvoiceNo", "exploded").show(2)
    // in Scala
    import org.apache.spark.sql.functions.map
    df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map")).show(2)
    // in Scala
    df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
      .selectExpr("complex_map['WHITE METAL LANTERN']").show(2)
    // in Scala
    df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
      .selectExpr("explode(complex_map)").show(2)
    // in Scala
    val jsonDF = spark.range(1).selectExpr("""
  '{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")
    // in Scala
    import org.apache.spark.sql.functions.{get_json_object, json_tuple}
    jsonDF.select(
      get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column",
      json_tuple(col("jsonString"), "myJSONKey")).show(2)
    jsonDF.selectExpr(
      "json_tuple(jsonString, '$.myJSONKey.myJSONValue[1]') as column").show(2)
    // in Scala
    import org.apache.spark.sql.functions.to_json
    df.selectExpr("(InvoiceNo, Description) as myStruct")
      .select(to_json(col("myStruct")))
    // in Scala
    import org.apache.spark.sql.functions.from_json
    import org.apache.spark.sql.types._
    val parseSchema = new StructType(Array(
      new StructField("InvoiceNo",StringType,true),
      new StructField("Description",StringType,true)))
    df.selectExpr("(InvoiceNo, Description) as myStruct")
      .select(to_json(col("myStruct")).alias("newJSON"))
      .select(from_json(col("newJSON"), parseSchema), col("newJSON")).show(2)
    // in Scala
    val udfExampleDF = spark.range(5).toDF("num")
    def power3(number:Double):Double = number * number * number
    power3(2.0)
    // in Scala
    import org.apache.spark.sql.functions.udf
    val power3udf = udf(power3(_:Double):Double)
    // in Scala
    udfExampleDF.select(power3udf(col("num"))).show()
    // in Scala
    spark.udf.register("power3", power3(_:Double):Double)
    udfExampleDF.selectExpr("power3(num)").show(2)
    // COMMAND ----------

  }
}
