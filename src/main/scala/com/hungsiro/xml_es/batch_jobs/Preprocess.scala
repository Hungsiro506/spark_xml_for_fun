package com.hungsiro.xml_es.batch_jobs

import java.io.{File, FileOutputStream, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.concurrent.Executors
import scala.io._
/**
  * Created by hungdv on 14/07/2017.
  */
object Preprocess {
  def main(args: Array[String]): Unit = {
    // TESTING PURPOSE
/*    val path: Path = Paths.get("/home/hungdv/workspace/xml-parser/src/main/resources/raw.XML")
    val lines = Files.readAllLines(path,StandardCharsets.UTF_8)
    val lineContainsDoctype = lines.get(1)
    System.out.println(lineContainsDoctype)
    val data:String = "<!DOCTYPE us-patent-grant>"
    lines.set(1,data)
    val newLine = lines.get(1)
    System.out.println(newLine)*/

    System.out.println("--------------------------------------------------------------")
    System.out.println("--------------------------------------------------------------")

   /* val folder = "/home/hungdv/Downloads/xml-spark/New"
    val files = getAllFilesInAGivenFolder(folder)
    val data = "<!DOCTYPE us-patent-grant>"
    files.foreach{file =>
      println("Process file : " + file.toString)
      // Replace line number 1 with new content as data.
      // The line of a file starts with index 0.
      replaceAGivenLineInFile(file.toString,1,data)
    }*/
    // MERGING FILE
/*    val testfolder = "/home/hungdv/Downloads/xml_data/file4"
    val megedfolder = "/home/hungdv/Downloads/xml_data/meged/"
    val chunksize = 10
    mergerFile(testfolder,chunksize,megedfolder)*/

    // FILTER FILE

/*    val testfile = "/home/hungdv/Downloads/xml_data/test/test2.txt"
    val stringFilter = "hung"
    removeCertainLine(testfile,stringFilter)
    readFile(testfile)*/
    // FILTER FILES CONCURREN
    //val data = "/home/hungdv/Downloads/xml_data/test/"
    //val data = "/home/hungdv/Downloads/xml_data/merge_test/"
/*    val data = "/home/hungdv/Downloads/xml_data/meged/"
    val filterCharecters = "<?"
    val files: Array[File] = getAllFilesInAGivenFolder(data)
    println(files.length)
    val numThreads = 4
    val executorService = Executors.newFixedThreadPool(numThreads)

    for(i <- 0 to (numThreads -1) )  {
      executorService.execute(new FileFilter(i,files,filterCharecters,numThreads))
    }*/

    val rawPathFile = "/home/hungdv/Downloads/xml_data/file4/"
    val megedfolder = "/home/hungdv/Downloads/xml_data/meged/"
    val numberThread = 4
    val chunkSize = 10
    preprocess(rawPathFile,megedfolder,chunkSize,numberThread)

  }
  def preprocess(rawPathFile: String,mergedFolder:String,chunkSize: Int,numThreads: Int): Unit={
    System.out.println("--------------------------------------------------------------")
    System.out.println("--------------------------------------------------------------")

    /* val folder = "/home/hungdv/Downloads/xml-spark/New"
     val files = getAllFilesInAGivenFolder(folder)
     val data = "<!DOCTYPE us-patent-grant>"
     files.foreach{file =>
       println("Process file : " + file.toString)
       // Replace line number 1 with new content as data.
       // The line of a file starts with index 0.
       replaceAGivenLineInFile(file.toString,1,data)
     }*/
    // MERGING FILE
    mergerFile(rawPathFile,chunkSize,mergedFolder)
    // FILTER FILE
    /*    val testfile = "/home/hungdv/Downloads/xml_data/test/test2.txt"
        val stringFilter = "hung"
        removeCertainLine(testfile,stringFilter)
        readFile(testfile)*/
    // FILTER FILES CONCURREN
    //val data = "/home/hungdv/Downloads/xml_data/test/"
    //val data = "/home/hungdv/Downloads/xml_data/merge_test/"
    val data = mergedFolder
    //val data = "/home/hungdv/Downloads/xml_data/meged/"
    val filterCharecters = "<?"
    val files: Array[File] = getAllFilesInAGivenFolder(data)
    println(files.length)
    val executorService = Executors.newFixedThreadPool(numThreads)
    for(i <- 0 to (numThreads -1) )  {
      executorService.execute(new FileFilter(i,files,filterCharecters,numThreads))
    }
  }
  /**
    * Get all absolute path file of all files in a given folder.
    * @param path
    * @return Array of all Files.
    */
  def getAllFilesInAGivenFolder(path: String): Array[File] ={
    val files: Array[File] = new java.io.File(path).listFiles()
    files
  }

  /**
    * Replace line in file
    * @param pathFile
    * @param numberOfLine
    * @param newData
    */
  def replaceAGivenLineInFile(pathFile: String,numberOfLine: Int,newData: String): Unit ={
    val path: Path = java.nio.file.Paths.get(pathFile)
    val lines = Files.readAllLines(path,StandardCharsets.UTF_8)
    lines.set(numberOfLine,newData)
    Files.write(path,lines,StandardCharsets.UTF_8)
    System.out.println("Finish -- " + pathFile)
  }
  def removeGivenLineOfFile(pathFile: String,numberOfLine: Int) : Unit = {
    val path: Path = java.nio.file.Paths.get(pathFile)
    val lines = Files.readAllLines(path,StandardCharsets.UTF_8)
    lines.remove(numberOfLine)
    Files.write(path,lines,StandardCharsets.UTF_8)
    System.out.println("Finish -- " + pathFile)
  }
  def mergerFile(path: String, chunkSize: Int,mergedFolderPath: String): Unit ={
    val files: Array[File] = getAllFilesInAGivenFolder(path)
    var count = 0
    var mergedFilePath = mergedFolderPath + "merged_"+(count)
    val numberOfFiles = files.length
    System.out.println(numberOfFiles)
    val f = new File(mergedFilePath)

    f.getParentFile().mkdirs()
    f.createNewFile()
    files.foreach{
      file =>

        if(count == numberOfFiles || count %10 == 0){
          new PrintWriter(new FileOutputStream(new File(mergedFilePath), true)) { write("</docs>"); close }
          System.out.println("Finish -- " + mergedFilePath)
          mergedFilePath = mergedFolderPath + "merged_"+count+".xml"
          val f = new File(mergedFilePath)
          f.getParentFile().mkdirs()
          f.createNewFile()
          new PrintWriter(mergedFilePath) { write("<!DOCTYPE docs>\n"); write("<docs>\n"); close }

        }

        val lines = Files.readAllLines(java.nio.file.Paths.get(file.getAbsolutePath),StandardCharsets.UTF_8)
        Files.write(java.nio.file.Paths.get(mergedFilePath),lines,StandardCharsets.UTF_8,StandardOpenOption.APPEND)
        System.out.println("Finish -- " + count)
        count = count +1
    }
    new PrintWriter(new FileOutputStream(new File(mergedFilePath), true)) { write("</docs>"); close }
    System.out.println("Finish -- " + mergedFilePath)
  }
  def removeCertainLine(pathFile: String, filterCharaters: String): Unit={
    val fileFiltered: java.io.File = new File(pathFile + "_tmp")
    val writer = new PrintWriter(fileFiltered)
     try{
       val textFromFile = scala.io.Source.fromFile(pathFile)
       val filter = textFromFile.getLines
         .filter(p => !(p contains filterCharaters))
         .foreach{
              x => writer.println(x)
         }
       textFromFile.close()
     }catch{
       case e: java.io.FileNotFoundException => println("File note found! I don't give a shit.")
     } finally {

     }


    writer.flush()
    writer.close()
    fileFiltered.renameTo(new File(pathFile))
    //ileFiltered.
  }
  def readFile(path: String): Unit={
    val file = scala.io.Source.fromFile(path).getLines().foreach(println(_))
  }

}
