package com.hungsiro.xml_es.batch_jobs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by hungdv on 14/07/2017.
  */
object XmlParserTest {
  //val master = "yarn"
  val master = "local[4]"
  val appName = "xml-parser"
  //val ElasticSearchHost = "search-test2-i5kcyxqrnbzrxmkdwkjbr5325m.us-west-2.es.amazonaws.com"
  //val ElasticSearchHost = "localhost"
  val ElasticSearchHost = "search-thesis-demo-g6towezebzbr6glnowgawgbwua.us-west-2.es.amazonaws.com"


  def resolveHostToIpIfNecessary(host:String ):String= {
    val hostAddress = java.net.InetAddress.getByName(host).getHostAddress()
    return hostAddress
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkConfig = new SparkConf()
    sparkConfig
      //.set("es.port","9200")
      .set("es.port","80")
      //.set("es.nodes","172.30.41.37")
      //.set("es.nodes","localhost")
      .set("es.http.timeout","5m")
      .set("es.nodes.wan.only","true")
      .set("es.scroll.size","50")
      .set("es.index.auto.create", "true")
      .set("es.nodes",resolveHostToIpIfNecessary(ElasticSearchHost))


    val sparkSession = SparkSession.builder().config(sparkConfig).appName(appName).master(master).getOrCreate()
/*    val rawData = sparkSession.sparkContext.textFile("/home/hungdv/Downloads/US20100019657A1-20100128.XML")

    rawData.filter(line => !(line.contains("<?"))).coalesce(1).saveAsTextFile("/home/hungdv/workspace/xml-parser/src/main/resources/filter3.xml")*/
   /*for(i <- 1 to 1){
      parseAndSave(i,sparkSession)
    }*/
    //parseAndSave("/home/hungdv/Downloads/xml_data/meged/folder1/folder2/*.xml",sparkSession)




    //val rawData = sparkSession.sparkContext.textFile("/home/hungdv/workspace/xml-parser/src/main/resources/US08983153-20150317.XML,/home/hungdv/workspace/xml-parser/src/main/resources/US08983140-20150317.xml")
    //val rawData = sparkSession.sparkContext.textFile("/home/hungdv/workspace/xml-parser/src/main/resources/US08983153-20150317.XML")

    //rawData.filter(line => !(line.contains("<?"))).coalesce(1).saveAsTextFile("/home/hungdv/workspace/xml-parser/src/main/resources/filter2.xml")
    //
    // PARSING SAMPLE AND GET SCHEMA.
  ////////////
  val paperXML: DataFrame = sparkSession.read.format("com.databricks.spark.xml")
    .option("rowTag","us-patent-application")
    //.option("rowTag","us-patent-grant")
    .option("excludeAttribute","true")
    .option("samplingRatio","1")
    // .load("/home/hungdv/Downloads/xml_data/merge_test/merged_0.xml")
    //.load("/home/hungdv/Downloads/xml_data/merge_test/*.xml")
    .load("/home/hungdv/workspace/xml-parser/src/main/resources/filter3.xml/part-00000")


    paperXML.show()
    println(paperXML.count())


    val schema: StructType = paperXML.schema

  /*  val paperRDD = paperXML.rdd.map{row =>
      PaperMetadata(row.getString(0),row.getString(1),row.getString(2),row.getString(3),row.getString(4),row.getString(6))
    }
    paperRDD.foreach(println(_))*/

    //Folder1 :
//    for(i <-1 to 471){
//      val path = "/home/hungdv/Downloads/xml_data/meged/folder1/folder" + i + "/*.xml"
//      try{
//        parseAndSave(path,sparkSession,schema)
//        System.out.println("File number " + i + " is done!")
//     }catch{
//       case e: Exception => println("Error in " + i + " with error: " + e.toString)
//      }
//    }
    // Folder2 -> 7
    for(i <- 1 to 1){
      for(j <-1 to 471){
        val path = "/home/hungdv/Downloads/xml_data/meged/folder" + i + "/folder" + j + "/*.xml"
        try{
          println( "Process : " +  path)
          parseAndSave(path,sparkSession,schema)

        }catch{
          case e: Exception => println("Error in " + i + " with error: " + e.toString)
        }
      }


    }

  }
  def parseAndSave(i: Int,sparkSession: SparkSession ): Unit ={
    val pathString = "/home/hungdv/Downloads/xml_data/meged/folder" + i +"/*.xml"

    val paperXML: DataFrame = sparkSession.read.format("com.databricks.spark.xml")
      .option("rowTag","us-patent-application")
      //.option("rowTag","us-patent-grant")
      .option("excludeAttribute","true")
      .option("samplingRatio","1")
      // .load("/home/hungdv/Downloads/xml_data/merge_test/merged_0.xml")
      //.load("/home/hungdv/Downloads/xml_data/merge_test/*.xml")
      .load(pathString)
    //.load("/home/hungdv/Downloads/xml_data/meged/*.xml")
    import org.elasticsearch.spark.sql._
    paperXML.saveToEs("xmltest4/paper1")
    System.out.println("Folder " + i+ " is done!")

  }
  def parseAndSave(path: String,sparkSession: SparkSession ): Unit ={

    val paperXML: DataFrame = sparkSession.read.format("com.databricks.spark.xml")
      .option("rowTag","us-patent-application")
      //.option("rowTag","us-patent-grant")
      .option("excludeAttribute","true")
      .option("samplingRatio","1")
      //.option("nullValue","n/a")
      // .load("/home/hungdv/Downloads/xml_data/merge_test/merged_0.xml")
      //.load("/home/hungdv/Downloads/xml_data/merge_test/*.xml")
      .load(path)
    //.load("/home/hungdv/Downloads/xml_data/meged/*.xml")
    import org.elasticsearch.spark.sql._
    paperXML.saveToEs("xmltest4/paper1")
    System.out.println("Folder " + path+ " is done!")

  }
  def parseAndSave(path: String,sparkSession: SparkSession,pschema: StructType ): Unit ={

    val paperXML: DataFrame = sparkSession.read.format("com.databricks.spark.xml")
      .option("rowTag","us-patent-application")
      //.option("rowTag","us-patent-grant")
      .option("excludeAttribute","true")
        .schema(pschema)
      //.option("nullValue","n/a")
      .load(path)
    import org.elasticsearch.spark.sql._
    paperXML.saveToEs("xmltest5/paper1")
    System.out.println("Folder " + path+ " is done!")

  }

}
