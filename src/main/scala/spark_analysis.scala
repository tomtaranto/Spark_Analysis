import org.apache.spark.sql.SparkSession

object spark_analysis extends App{

  val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
}
