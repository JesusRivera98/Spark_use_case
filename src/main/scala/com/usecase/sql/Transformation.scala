package com.usecase.sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat, count, desc, lit, rank, row_number, sum}

object Transformation {

  def addLiteralColumn(data: DataFrame): DataFrame = {
    data.select(col("*"), lit(1).alias("literal"))
  }

  def createTop(data: DataFrame): DataFrame = {
    val total = data.count()
    val windowSpec =  Window.partitionBy("total_records").orderBy(desc("records"))
    val windowSpec2 =  Window.partitionBy("noc").orderBy(desc("records"))
    data.groupBy("noc")
      .agg(count("noc") as "records")
      .withColumn("total_records",lit(total))
      .withColumn("rank", rank().over(windowSpec))//dense_rank()
//    data
//      .withColumn("records", count("noc").over(windowSpec2))
//      .withColumn("total_records", sum("}records").over(windowSpec2))
//      .withColumn("rank", rank().over(windowSpec))
  }

  def getTableFiltered(data: DataFrame):DataFrame = {
    data.where("noc == 'Japan'")
  }

  def createPercent(data: DataFrame):DataFrame = {
    data.withColumn("percent_records", (col("records")*100)/col("total_records"))
  }

  def createMessage(data: DataFrame):DataFrame = {
    //data.withColumn("message", lit(s"Japan in place ${col("rank_medals_number")} with ${col("medals_number")} won medals!"))
    val result = data.withColumn("message",
      concat(
        col("noc"),
        lit(" in place ") ,
        col("rank_by_total"),
        lit(" with "),
        col("total"),
        lit(" won medals!")))
    result.select("message").show(false)
    result
  }

  def joinObjects(data:DataFrame*):DataFrame={
      val joins = data(0).alias("a").join(
        data(1).alias("b"),
        //data(0)("noc") === data(1)("noc")
        Seq("noc")
      ).join(
        data(2).alias("c"),
        //data(0)("noc") === data(2)("noc")
        Seq("noc")
      )
    joins.show(false)

    //cleanJoins.show(false)
//    createMessage(renameColums(joins))
    renameColums(createMessage(joins))
  }
  def renameColums(data: DataFrame): DataFrame={
    data.select(
      col("noc"),
//      col("a.rank").as(rank_medals_number),
//      col("gold"),
//      col("silver"),
//      col("bronze"),
      col("total")  as "medals_number",
      col("rank_by_total") as "rank_medals_number" ,
      col("b.records").as("athletes_number"),
      //col("b.total_records").as("medals_total_records"),
      col("b.percent_records").as("athletes_percent_number"),
      col("b.rank").as("rank_athletes_number"),
      col("c.records").as("coaches_number"),
      //col("c.total_records").as("coaches_number"),
      col("c.percent_records").as("coaches_percent_number"),
      col("c.rank").as("rank_coaches_number"),
      col("message")
    )
//    data.select(
//      col("noc"),
//      col("a.rank").as("rank_medals"),
//      col("gold"),
//      col("silver"),
//      col("bronze"),
//      col("total"),
//      col("rank_by_total"),
//      col("b.records").as("medals_records"),
//      col("b.total_records").as("medals_total_records"),
//      col("b.rank").as("athletes_rank"),
//      col("b.percent_records").as("athletes_percent_records"),
//      col("b.records").as("athletes_records"),
//      col("c.total_records").as("coaches_total_records"),
//      col("c.rank").as("coaches_rank"),
//      col("c.percent_records").as("coaches_percent_records")
//    )
  }
  
}
