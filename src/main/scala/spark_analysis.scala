import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.log4j.Logger
import org.apache.log4j.Level


object spark_analysis extends App{
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  println("starting Spark Session")
  val spark = SparkSession.builder.appName("Simple Application").master("local[*]")getOrCreate()
  //val df = spark.read.format("json").load("/home/ttaranto/hadoop_data/*.json")


  println("Chargement des données")
  val df1 = spark.read.format("json").load("hdfs://localhost:9000/tmp/*.json")
  //val df1 = spark.read.format("json").load("/home/ttaranto/hadoop_data/mySample*.json")
  println("données chargées")
  // Pourcentage de citoyen trop contents


  println("Explosion des données")
  val df = df1.withColumn("personne",explode(arrays_zip(col("list_id"),col("list_nom"),col("list_prenom"),col("list_positivite")))).select(col("personne.list_id").as("id"),col("personne.list_nom").as("nom"),col("personne.list_prenom").as("prenom"),col("personne.list_positivite").as("positivite"), col("battery"), col("id_drone"),col("timestamp"), col("ville"))

  val ratio: Float = df1.select(explode(col("list_positivite"))).where(col("col")>90).count.toFloat / df1.select(explode(col("list_positivite"))).count.toFloat
  println("ratio de personnes trop contentes : ",ratio)

  // La ville avec le plus de citoyen content
  println("nombre de citoyens trop contents par ville : ")
  df.where(col("positivite") > 90).groupBy(col("ville")).count.orderBy(desc("count")).show

  //Le citoyen le plus content (soit par moyenne de content, soit par max_occurence de content)
  println("Les citoyens les plus contents sont : ")
  df.where(col("positivite") > 90).groupBy("id").agg(count("id").as("nombre_de_depassement"),first("nom").as("nom"),first("prenom").as("prenom"),first("ville").as("ville")).orderBy(desc("nombre_de_depassement")).show(20)

  //L'heure ou les citoyens sont les plus contents
  //Convertisseur string -> timestamp
  val df_corrected = df.withColumn("new_timestamp",to_timestamp(col("timestamp"),"dd/MM/yyyy hh:mm a"))
  println("Heures où il y a le plus de citoyens trop contents : ")
  df_corrected.groupBy(hour(col("new_timestamp")).alias("heure_depassement")).count.orderBy(desc("count")).show(30)


}
