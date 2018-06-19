package com.cffex.ch08

import org.apache.spark.sql.SparkSession

object Joins {
  def main(args:Array[String]): Unit ={
    val spark = SparkSession.builder()
      .appName("ch08")
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._

    // in Scala
    val person = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Armbrust", 1, Seq(250, 100)))
      .toDF("id", "name", "graduate_program", "spark_status")

    val graduateProgram = Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley"))
      .toDF("id", "degree", "department", "school")
    val sparkStatus = Seq(
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor"))
      .toDF("id", "status")
    // COMMAND ----------
    person.createOrReplaceTempView("person")
    graduateProgram.createOrReplaceTempView("graduateProgram")
    sparkStatus.createOrReplaceTempView("sparkStatus")
    // in Scala
    val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
    // in Scala
    val wrongJoinExpression = person.col("name") === graduateProgram.col("school")
    // COMMAND ----------
    person.join(graduateProgram, joinExpression).show()
    // COMMAND ----------
    var joinType = "inner"
    // COMMAND ----------
    person.join(graduateProgram, joinExpression, joinType).show()
    // COMMAND ----------
    joinType = "outer"
    // COMMAND ----------
    person.join(graduateProgram, joinExpression, joinType).show()

    joinType = "left_outer"

    graduateProgram.join(person, joinExpression, joinType).show()

    joinType = "right_outer"

    person.join(graduateProgram, joinExpression, joinType).show()
    // COMMAND ----------
    joinType = "left_semi"
    // COMMAND ----------
    graduateProgram.join(person, joinExpression, joinType).show()
    // in Scala
    val gradProgram2 = graduateProgram.union(Seq(
      (0, "Masters", "Duplicated Row", "Duplicated School")).toDF())

    gradProgram2.createOrReplaceTempView("gradProgram2")
    // COMMAND ----------
    gradProgram2.join(person, joinExpression, joinType).show()
    // COMMAND ----------
    joinType = "left_anti"
    graduateProgram.join(person, joinExpression, joinType).show()

    joinType = "cross"
    graduateProgram.join(person, joinExpression, joinType).show()
    person.crossJoin(graduateProgram).show()

    import org.apache.spark.sql.functions.expr

    person.withColumnRenamed("id", "personId")
      .join(sparkStatus, expr("array_contains(spark_status, id)")).show()
    // COMMAND ----------
    val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")

    val joinExpr = gradProgramDupe.col("graduate_program") === person.col(
      "graduate_program")
    // COMMAND ----------
    person.join(gradProgramDupe, joinExpr).show()
    // COMMAND ----------
    person.join(gradProgramDupe, joinExpr).select("graduate_program").show()
    // COMMAND ----------
    person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()
    // COMMAND ----------
    person.join(gradProgramDupe, joinExpr).drop(person.col("graduate_program"))
      .select("graduate_program").show()
    // COMMAND ----------
    val joinExpr1 = person.col("graduate_program") === graduateProgram.col("id")
    person.join(graduateProgram, joinExpr1).drop(graduateProgram.col("id")).show()
    // COMMAND ----------
    val gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
    val joinExpr2 = person.col("graduate_program") === gradProgram3.col("grad_id")
    person.join(gradProgram3, joinExpr2).show()
    // COMMAND ----------
    val joinExpr3 = person.col("graduate_program") === graduateProgram.col("id")
    person.join(graduateProgram, joinExpr3).explain()
    // COMMAND ----------
    import org.apache.spark.sql.functions.broadcast
    val joinExpr4 = person.col("graduate_program") === graduateProgram.col("id")
    person.join(broadcast(graduateProgram), joinExpr4).explain()
    // COMMAND ----------
  }
}
