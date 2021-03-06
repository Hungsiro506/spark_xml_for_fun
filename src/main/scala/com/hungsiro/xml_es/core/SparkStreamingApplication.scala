package com.hungsiro.xml_es.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 10/03/2017.
  */
trait SparkStreamingApplication extends SparkApplication{
  def streamingBatchDuration: FiniteDuration

  def streamingCheckpointDir: String
  //NEw API - Spark Session. - not tested yet
  def withSparkStreamingContext(f: (SparkSession,StreamingContext)=> Unit): Unit = {
    withSparkSession{
      sparkSession =>
        val sparkContext = sparkSession.sparkContext
        //sparkContext.setLogLevel("WARN")
        val streamingContext = new StreamingContext(sparkContext,Seconds(streamingBatchDuration.toSeconds))
        streamingContext.checkpoint(streamingCheckpointDir)

        f(sparkSession,streamingContext)

        streamingContext.start()
        streamingContext.awaitTermination()

    }
  }
  // OLD API
/*  def withSparkStreamingContext(f: (SparkContext, StreamingContext) => Unit): Unit = {
    withSparkContext { sc =>
      val ssc = new StreamingContext(sc, Seconds(streamingBatchDuration.toSeconds))
      ssc.checkpoint(streamingCheckpointDir)

      f(sc, ssc)

      ssc.start()
      ssc.awaitTermination()
    }
  }*/
}
