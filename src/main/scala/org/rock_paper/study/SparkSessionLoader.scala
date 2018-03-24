package org.rock_paper.study
/**
  * @author Ankit Beohar
  * This is to create Spark Session
  *
  * */

import org.apache.spark.sql.SparkSession
import org.slf4j.{ LoggerFactory, Marker, Logger => Underlying }
import java.io.IOException
object SparkSessionLoader {

  val logger = LoggerFactory.getLogger(SparkSessionLoader.getClass)

  def getSparkSession():SparkSession={
    try{
      val sc = SparkSession
        .builder()
        .appName("Spark CSV Reader")
        .config("spark.master", "local")
        .getOrCreate()
      return sc
    }
    catch{
      case ioe: IOException => logger.error("Unable to Create Spark Session")
        return null
    }

  }
  }
