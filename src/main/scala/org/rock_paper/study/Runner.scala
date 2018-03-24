package org.rock_paper.study
/**
  * @author Ankit Beohar
  *This is a runner code and
  * all child or dependent object call from here
  *
  * */


import org.apache.spark.sql
import org.slf4j.{ LoggerFactory, Marker, Logger => Underlying }

import java.io.IOException

object Runner {
  val logger = LoggerFactory.getLogger(Runner.getClass)
  val sc = SparkSessionLoader.getSparkSession()
  def main(args: Array[String]) {
    try{
      //read all data one by one and get the DF
/*
      val salesRawData = SalesReader.readData("C:/Users/Dell/Pictures/Saved Pictures/Sprituality/study/Sales.tsv")
      val complaintsRawData = ComplaintsReader.readData("C:/Users/Dell/Pictures/Saved Pictures/Sprituality/study/Complaints.tsv")
      val productionRawData = ProductionReader.readData("C:/Users/Dell/Pictures/Saved Pictures/Sprituality/study/Production_logs.tsv")*/

      val salesRawData = SalesReader.readData(args(0))
      val complaintsRawData = ComplaintsReader.readData(args(1))
      val productionRawData = ProductionReader.readData(args(2))

      salesRawData.createOrReplaceTempView("salesData")
      complaintsRawData.createOrReplaceTempView("complaintData")
      productionRawData.createOrReplaceTempView("productionData")

      //run 2 sql to modify data as per our need
      val deriveProductionData=sc.sql(PropertiesLoader.properties.getProperty("sqlA"))
      val deriveSalesComplaint=sc.sql(PropertiesLoader.properties.getProperty("sqlB"))

      //register those 2 sql output as temp view
      deriveProductionData.createOrReplaceTempView("deriveProductionDataDf")
      deriveSalesComplaint.createOrReplaceTempView("deriveSalesComplaintDf")

      //run all three kpi/metrics sql
      val metric1=sc.sql(PropertiesLoader.properties.getProperty("metricsql1"))
      val metric2=sc.sql(PropertiesLoader.properties.getProperty("metricsql2"))
      val metric3=sc.sql(PropertiesLoader.properties.getProperty("metricsql3"))

      /*metric1.show(5)
      metric2.show(5)
      metric3.show(5)*/
      /*metric1.coalesce(1).write.csv("C:/Users/Dell/Pictures/Saved Pictures/Sprituality/study/ITEM_WISE_DEFECTED")
      metric2.coalesce(1).write.csv("C:/Users/Dell/Pictures/Saved Pictures/Sprituality/study/UNIT_MORE_THAN_20%_DEFECTED")
      metric3.coalesce(1).write.csv("C:/Users/Dell/Pictures/Saved Pictures/Sprituality/study/QC_DEFECTED")*/

      //outfile all three separately
      metric1.coalesce(1).write.option("header","true").option("delimiter","|").csv(args(3))
      metric2.coalesce(1).write.option("header","true").option("delimiter","|").csv(args(4))
      metric3.coalesce(1).write.option("header","true").option("delimiter","|").csv(args(5))
      logger.debug("Process Done Check Data in Outfile")
    }
    catch{
      case e: Exception => logger.error("Unable to run Scala Job"+e)
        return e
    }

  }
  // evaluate command line arguments and exit if any of it unavailable
  def evaluateCommandLine(args: Array[String]){
    if (args.length < 6) {
      logger.error("Invalid number of parameters")
      System.err.println("Usage: Travel Agency Data <sales file name> " +
        "<complaint file name> <production file name> <metricA file name> <metricB file name> <metricC file name>")
      System.exit(1)
    }
  }

}