package org.rock_paper.study
/**
  * @author Ankit Beohar
  * This is to read Complaints data and return for analysis
  *
  * */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.util.Calendar
import org.apache.spark.sql.functions._
import org.slf4j.{LoggerFactory, Marker, Logger => Underlying}
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.types._

object ComplaintsReader {
  val logger = LoggerFactory.getLogger(SalesReader.getClass)
  val startTime = Calendar.getInstance().getTimeInMillis
  def readData(args: String):DataFrame= {
    val sc = SparkSessionLoader.getSparkSession()

    //schema for complaint data
    val complaint_schema = new StructType()
      .add("invoice_id", StringType,true)
      .add("defective_items", StringType)

      //read data from file
    val ReadComplaintDF = sc.read.format("csv")
      .option("sep", "\t").option("quoteMode", "ALL").option("escape", "\"")
      .schema(complaint_schema)
      .option("header", "false")
      .load(args)

    //register as a temp view
    ReadComplaintDF.createOrReplaceTempView("complaintTab")

    //run sql to get raw data
    val complaintDataDf=sc.sql("select * from complaintTab")
    val endTime = Calendar.getInstance().getTimeInMillis
    logger.debug("Complaint Reader Processing Time===>> "+ ((endTime-startTime)/1000.0))
    //return raw data as DF
    return complaintDataDf
  }
}
