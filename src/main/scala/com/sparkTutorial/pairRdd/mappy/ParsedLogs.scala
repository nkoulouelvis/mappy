package com.sparkTutorial.pairRdd.mappy
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark._

import scala.reflect.internal.util.Collections

object ParsedLogs {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("ParsedLogs").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val logsRDD = sc.textFile("in/tornik-map-20171006.10000.tsv")
    val urlRDD = logsRDD.flatMap(line => line.split(" /")).filter(line =>hasMap(line))
    val urlViewRDD =urlRDD.map(url => url.split("\t")(1))
    val goodViewRDD = urlViewRDD.filter(url => isUrl(url))
    val viewModeRDD = goodViewRDD.map(view => view.split("/")(4))
    val viewModeZomRDD = goodViewRDD.map(view => view.split("/")(6))
    val viewModeCollection = viewModeRDD.collect()
    val zoomCollection = viewModeZomRDD.collect()
    println("--------Exercice1-------------------")
    parseLogs(viewModeCollection)
    println("-------------------------------------")
    println("---------Exercice2-------------------")
    parseModeZomLogs(viewModeCollection,zoomCollection)
  }

  /**
    * 
    * @param line
    * @return
    */
  def isUrl (line : String): Boolean = line.split("/").size ==9

  /**
    * 
    * @param line
    * @return
    */
  def hasMap (line : String) : Boolean = line.split("\t").size==2

  /**
    * 
    * @param logs
    */
  def parseLogs(logs:Array[String]):Unit ={
    var count:Int=1
    for(i <- 1 to  logs.length-1){

      if(logs(i)==logs(i-1)){
        count +=1
      }
      else {
        println(logs(i-1)+" "+ count)
        count=1
      }
    }
    if(logs(logs.length-1)!=logs(logs.length-2)) {
      println(logs(logs.length-1) +" "+ 1)
    } else { println(logs(logs.length-1) +" "+ count)}
  }

  /**
    *
    * @param logs
    * @param zoom
    */
  def parseModeZomLogs(logs:Array[String],zoom:Array[String]):Unit ={
    var count:Int=1
    var zoomMode=""
    for(i <- 1 to  logs.length-1){
      if(logs(i)==logs(i-1)){
        count +=1
        if(zoomMode == ""){
          if(zoom(i-1)!=zoom(i))zoomMode=zoom(i-1)+","+zoom(i)
          else  zoomMode=zoom(i-1)
        }
        else if(!zoomMode.contains(zoom(i))) zoomMode+=","+zoom(i)

      }
      else {
        if(zoomMode == "")zoomMode=zoom(i-1)
        println(logs(i-1)+" "+ count +" "+zoomMode)
        count=1
        zoomMode=""


      }
    }
    if(logs(logs.length-1)!=logs(logs.length-2)) {
      println(logs(logs.length-1) +" "+ 1 +" " + zoom(logs.length-1))
    } else { println(logs(logs.length-1) +" "+ count +" " + zoomMode)}
  }
  
}
