package org.example

import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object DistCp {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("DistCP -m 10 -i input output ")
      System.exit(-1)
    }
    val options = new Options()
    options.addOption("i", "ignore failure ", false, "ignore failures")
    options.addOption("m", "max concurrence ", true, "max concurrence")
    val parser = new DefaultParser()
    val cmd = parser.parse(options, args)
    val IGNORE_FAILURE = cmd.hasOption("i")
    val maxCon = if (cmd.hasOption("m")) cmd.getOptionValue("m").toInt
    else 2
    val input = args(args.length - 2)
    val output = args(args.length - 1)
    val sparkConf = new SparkConf().setAppName("DistCp").setMaster("local")

    val sc = new SparkContext(sparkConf)

    val sf  = FileSystem.get(sc.hadoopConfiguration)

    val fileList = sf.listFiles(new Path(input),true)

    val arrBuf = ArrayBuffer[String]()

    while (fileList.hasNext){
      arrBuf.append(fileList.next().getPath.toString)
    }

    val rdd = sc.parallelize(arrBuf, maxCon)
    rdd.foreachPartition(it=>{
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      while(it.hasNext){
        val src = it.next()
        val tgt = src.replace(input,output)
        try{
          FileUtil.copy(fs,new Path(src),fs,new Path(tgt),false,conf)
        }catch {
          case ex:Exception =>
            if (IGNORE_FAILURE) print("ignore failure when copy")
            else throw ex
        }
      }
    })
  }

}
