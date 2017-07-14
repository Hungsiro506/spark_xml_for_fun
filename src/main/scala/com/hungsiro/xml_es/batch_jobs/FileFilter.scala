package com.hungsiro.xml_es.batch_jobs

import java.io.File

/**
  * Created by hungdv on 18/06/2017.
  */
class FileFilter(id : Int, files: Array[File],filterCharecters: String,numThreads: Int) extends Runnable{
    def run: Unit ={
      val bound = numThreads - id
      println("Thread id " + id)
      for( j <- id to (files.length - bound) by numThreads){
        val file = files(j).toString
        //println("Process file " + file )
        Preprocess.removeCertainLine(file,filterCharecters)
      }
      println("Thread id " + id + " is done!.")
    }
}
