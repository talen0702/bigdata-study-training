package org.example

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}


object InvertIndex {

  def main(args: Array[String]): Unit = {

    val input = args(0)

    val sparkConf = new SparkConf().setAppName("InvertIndex").setMaster("local")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val fileList = fs.listFiles(new Path(input),true)

    var rdd = sc.emptyRDD[(String,String)]

    while (fileList.hasNext){
      val path = new Path(fileList.next().getPath.toString)
      val fileName = path.getName

      rdd = rdd.union(sc.textFile(path.toString).flatMap(_.split("\\s+").map((fileName,_))))

    }

    val rdd2 = rdd.map(word=>{
      (word,1)
    }).reduceByKey(_+_)

    rdd2.foreach(print)

    val frdd = rdd2.map(data=>{
      (data._1._2,String.format("(%s,%s)",data._1._1,data._2.toString))
    }).reduceByKey(_+","+_)
      .map(word=>String.format("\"%s\",{%s}",word._1,word._2))

    frdd.foreach(print)
  }
}
