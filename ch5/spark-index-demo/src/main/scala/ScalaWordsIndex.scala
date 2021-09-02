import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

/**
 * 题目1
 * 只实现了第一个需求，第二个需求没有头绪
 */
object ScalaWordsIndex {

  private val NPARAMS = 1

  def main(args: Array[String]): Unit = {
    if (args.length != NPARAMS) {
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("scalaWordsIndex")
    val sc = new SparkContext(conf)
    val fileRdd = sc.textFile(args(0))
    val md = fileRdd.map(file=>file.split("\t"))
    //构造成文档ID，文件
    val md2 = md.map(item=>{(item(0),item(1))})
    //构造成词，文档ID
    val textRdd = md2.flatMap(file =>{
      val words = file._2.split(" ").iterator
      val list = mutable.LinkedList[(String,String)]((words.next(),file._1))
      var temp = list
      while(words.hasNext){
        temp.next = mutable.LinkedList[(String,String)]((words.next,file._1))
        temp = temp.next
      }
      list
    })
    //去重
    val result = textRdd.distinct()
    val resRdd = result.reduceByKey(_+" "+_)
    resRdd.saveAsHadoopFile("hdfs://master:9000/talen/wordsIndex")
    sc.stop()
  }
}
