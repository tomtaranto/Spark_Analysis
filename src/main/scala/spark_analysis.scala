import org.apache.spark.sql.SparkSession

object spark_analysis extends App{

  val spark = SparkSession.builder.appName("Simple Application").master("local[*]")getOrCreate()
  val df = spark.read.format("json").load("/home/ttaranto/hadoop_data/*.json")




}
