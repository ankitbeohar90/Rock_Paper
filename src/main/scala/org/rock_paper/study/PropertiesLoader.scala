package org.rock_paper.study
import java.util.Properties
/**
  * @author Ankit Beohar
  * This is to read sql queries from property file.
  * */
import scala.io.Source
object PropertiesLoader {
  var properties : Properties = null

    val url = getClass.getResource("/Query.properties")
   // println("==========>"+url)

    if (url != null) {
      val source = Source.fromURL(url)

      properties = new Properties()
      properties.load(source.bufferedReader())


  }

  /*def main(args: Array[String]): Unit = {
    println("sqlA check=======>"+PropertiesLoader.properties.getProperty("sqlA"))
  }
*/
}
