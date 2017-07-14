package com.hungsiro.xml_es.batch_jobs

/**
  * Created by hungdv on 14/07/2017.
  */
case class XMLParsingAndIngestConfig(
                                    sparkConfig: Map[String,String],
                                    rootFolder: String,
                                    mergedFolder: String,
                                    esIndex: String,
                                    fileChunkSizeLv1: Int,
                                    fileChunkSizeLv2: Int,
                                    sampleFile: String,
                                    rowTag: String
                                    ) extends  Serializable{}
object XMLParsingAndIngestConfig{
  import com.typesafe.config.{Config, ConfigFactory}
  import net.ceedubs.ficus.Ficus._

  def apply(): XMLParsingAndIngestConfig = apply(ConfigFactory.load())
  def apply(xmlConfig: Config): XMLParsingAndIngestConfig = {
    val config = xmlConfig.getConfig("parse_and_save")
    new XMLParsingAndIngestConfig(
      config.as[Map[String,String]]("sparkConfig"),
      config.as[String]("rawtFolder"),
      config.as[String]("mergedFolder"),
      config.as[String]("esIndex"),
      config.as[Int]("fileChunkSizeLv1"),
      config.as[Int]("fileChunkSizeLv2"),
      config.as[String]("sampleFile"),
      config.as[String]("rowTag")
    )
  }
}