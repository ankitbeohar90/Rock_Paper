package org.rock_paper.study
/**
  * @author Ankit Beohar
  * This is to read Sales data and return for analysis
  *
  * */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.util.Calendar
import org.apache.spark.sql.functions._
import org.slf4j.{LoggerFactory, Marker, Logger => Underlying}
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.types._


object SalesReader {
  val logger = LoggerFactory.getLogger(SalesReader.getClass)
  val startTime = Calendar.getInstance().getTimeInMillis
  def readData(args: String):DataFrame= {
    val sc = SparkSessionLoader.getSparkSession()
    //create schema for sales data
    val salesSchema = new StructType()
      .add(StructField("invoice_id", StringType, true))
      .add(StructField("customer_id", StringType, true))
      .add(StructField("items_summary",StringType,true))
      .add(StructField("batch_id", StringType, true))

    //read data in spark
    val ReadSalesDF = sc.read.format("csv")
      .option("sep", "\t").option("quoteMode", "ALL").option("escape", "\"")
      .schema(salesSchema)
      .option("header", "false")
      .load(args)

    // register as temp view
    ReadSalesDF.createOrReplaceTempView("salesTab")

    //get raw data
    val salesDataDf=sc.sql("select * from salesTab")

    val endTime = Calendar.getInstance().getTimeInMillis
    logger.debug("Sales Reader Processing Time===>> "+ ((endTime-startTime)/1000.0))

    //return raw data df
    return salesDataDf
  }

}