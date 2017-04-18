import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.Date
/**
  * Created by alex on 18/04/2017.
  */


  object SparkWordCount {
    def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder
        .master("local")
        .appName("Spark Graph Frames")
        .getOrCreate()


      val format = new SimpleDateFormat("hh")
      val textFile = spark.sparkContext.textFile("/Volumes/Transcend/Cours/spark/exercice_Spark/tp_spark/extract_simple_heure.txt")

      //exercice 1
      val url = textFile.flatMap(line => line.split("\r")).map(_.split(" "))
        .map(word => (word(0)+" "+word(2),1)).reduceByKey((a, b) => a+b)
      url.foreach(println)

      println("exercice 2")
      //exercice 2
      val urlUnique = textFile.flatMap(line => line.split("\r")).map(_.split(" "))
        .map(word => (word(0),word(2)))
      val unique = urlUnique.distinct().reduceByKey((a, b) => a +", "+ b)

      unique.foreach(println)
    }
  }
