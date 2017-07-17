package com.hungsiro.xml_es.core

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.collection.{Map, mutable}
import scala.concurrent.duration.FiniteDuration

/**  Singleton  object to get Broadcast variable.
  * Created by hungdv on 08/05/2017.
  */
// FIXME : Don't repeat your self!!!!!!!
object DurationBoadcast {

  @volatile private var instance: Broadcast[FiniteDuration] = null

  def getInstance(sc: SparkContext,duration: FiniteDuration): Broadcast[FiniteDuration] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.broadcast(duration)
        }
      }
    }
    instance
  }
}
//FIXME : use AbtractLogParser instead  of child class
/*
object ParserBoacast {

  @volatile private var instance: Broadcast[AbtractLogParser] = null

  def getInstance(sc: SparkContext,parser: AbtractLogParser): Broadcast[AbtractLogParser] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.broadcast(parser)
        }
      }
    }
    instance
  }
}
*/



object MapBroadcast {

  @volatile private var instance: Broadcast[Map[String,String]] = null

  def getInstance(sc: SparkContext,map: mutable.Map[String,String]): Broadcast[Map[String,String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.broadcast(map)
        }
      }
    }
    instance
  }
}



