import org.apache.spark.sql.SparkSession

/**
 * 题目2
 * 实现不完成，后期修改优化
 */
object ScalaSparkDistcp {

  private val NPARAMS = 1

  def main(args: Array[String]): Unit = {
    if (args.length != NPARAMS) {
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("scalaSparkDistcp")
      .getOrCreate()

    val fileListRDD = spark.sparkContext.wholeTextFiles(args(0),4)
    fileListRDD.foreach(item => {
      val fileRdd = spark.read.textFile(item._1+item._2).rdd
      fileRdd.saveAsTextFile("hdfs://master:9000/talen/distcp/"+item._1)
    })

    spark.stop()
  }
}
