package com.hungsiro.xml_es.batch_jobs

import com.hungsiro.xml_es.core.SparkApplication
import org.apache.log4j.{Level, Logger}

/**
  * Created by hungdv on 14/07/2017.
  */
class XmlParsingAndIndex(config: XMLParsingAndIngestConfig) extends SparkApplication{
  override def sparkConfig: Map[String, String] = config.sparkConfig
  def start(): Unit ={
    withSparkSession{ss =>
    ParseAndSave.run(ss,config)
    }
  }

}
object Driver{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val config = XMLParsingAndIngestConfig()
    val ingestJob = new XmlParsingAndIndex(config)
    ingestJob.start()
  }
}
