package org.rock_paper.study
/**
  * @author Ankit Beohar
  * This is to read Production data and return for analysis
  *
  * */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.util.Calendar
import org.apache.spark.sql.functions._
import org.slf4j.{LoggerFactory, Marker, Logger => Underlying}
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.types._


object ProductionReader {
  val logger = LoggerFactory.getLogger(SalesReader.getClass)
  val startTime = Calendar.getInstance().getTimeInMillis
  def readData(args: String):DataFrame= {
    val sc = SparkSessionLoader.getSparkSession()
    //schema for production data
    val production_schema = new StructType()
      .add("production_unit_id", StringType,true)
      .add("batch_id", StringType)
      .add("items_produced", StringType)
      .add("items_discarded", StringType)

    //read data from file
    val ReadProductionDF = sc.read.format("csv")
      .option("sep", "\t").option("quoteMode", "ALL").option("escape", "\"")
      .schema(production_schema)
      .option("header", "false")
      .load(args)

    ReadProductionDF.createOrReplaceTempView("productionTab")
    //run sql to get the raw data
    val productionDataDf=sc.sql("select * from productionTab")
    val endTime = Calendar.getInstance().getTimeInMillis
    logger.debug("Production Reader Processing Time===>> "+ ((endTime-startTime)/1000.0))
    //return raw data as temp view
    return productionDataDf
  }

}
