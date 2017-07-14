package com.hungsiro.xml_es.batch_jobs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

/**
  * Created by hungdv on 14/07/2017.
  */
object ParseAndSave {

  def run(ss: SparkSession, config: XMLParsingAndIngestConfig): Unit ={
    val megedFolder = config.mergedFolder
    val esIndex = config.esIndex
    val chunkSizeLv1 = config.fileChunkSizeLv1
    val chunkSizeLv2 = config.fileChunkSizeLv2
    val sample = config.sampleFile
    val rowTag = config.rowTag
    // Get schema from sample file
    val schema = getSchema(ss,sample,rowTag)
    // Apply schema and parse
    for(i <- 1 to chunkSizeLv1){
      for(j <-1 to chunkSizeLv2){
        val path = megedFolder +"folder" +i + "/folder" + j + "/*.xml"
        try{
          println( "Process : " +  path)
          parseAndSave(path,ss,schema,rowTag,esIndex)

        }catch{
          case e: Exception => println("Error in " + i + " with error: " + e.toString)
        }
      }
    }

  }

  private[this] def getSchema(sparkSession: SparkSession,sample: String,rowTag: String): StructType ={
    val paperXML: DataFrame = sparkSession.read.format("com.databricks.spark.xml")
      .option("rowTag",rowTag)
      //.option("rowTag","us-patent-grant")
      .option("excludeAttribute","true")
      .option("samplingRatio","1")
      // .load("/home/hungdv/Downloads/xml_data/merge_test/merged_0.xml")
      //.load("/home/hungdv/Downloads/xml_data/merge_test/*.xml")
      .load(sample)

    val schema: StructType = paperXML.schema
    schema
  }

  private[this] def parseAndSave(path: String,sparkSession: SparkSession,rowTag: String,esDestination :String): Unit ={

    val paperXML: DataFrame = sparkSession.read.format("com.databricks.spark.xml")
      .option("rowTag",rowTag)
      //.option("rowTag","us-patent-application")
      //.option("rowTag","us-patent-grant")
      .option("excludeAttribute","true")
      .option("samplingRatio","1")
      //.option("nullValue","n/a")
      .load(path)

    import org.elasticsearch.spark.sql._
    paperXML.saveToEs(esDestination)
    System.out.println("Folder " + path+ " is done!")
  }

  private[this] def parseAndSave(path: String,sparkSession: SparkSession,pschema: StructType,rowTag: String,esDestination :String): Unit = {

    val paperXML: DataFrame = sparkSession.read.format("com.databricks.spark.xml")
      .option("rowTag", rowTag)
      //.option("rowTag","us-patent-grant")
      .option("excludeAttribute", "true")
      .schema(pschema)
      //.option("nullValue","n/a")
      .load(path)

    import org.elasticsearch.spark.sql._
    paperXML.saveToEs(esDestination)
    System.out.println("Folder " + path + " is done!")
  }

  private def resolveHostToIpIfNecessary(host:String ):String= {
    val hostAddress = java.net.InetAddress.getByName(host).getHostAddress()
    return hostAddress
  }
}
