package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object PokemonBasicETLOperations {


  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("PokemonBasicETLOperations")
      .master("local[*]")
      .getOrCreate()

    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.



    var pokemon = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/Pokemon.csv")

    // There are lots of other ways to make a DataFrame.
    // For example, spark.read.json("json file path")
    // or sqlContext.table("Hive table name")



    pokemon = pokemon.withColumnRenamed("Type 1", "Type_1")
      .withColumnRenamed("Type 2", "Type_2")
      .withColumnRenamed("Sp. Atk", "Sp_Atk")
      .withColumnRenamed("Sp. Def", "Sp_Def")

    var max_attack_per_type = pokemon.filter(col("Generation") === 1 and col("Name")
      .like("%Mega%") === false)
      .select(col("Name"),
        col("Type_1").alias("Type"),
        col("Attack"),
        col("Sp_Atk"),
        row_number().over(Window.partitionBy(col("Type_1")).orderBy(col("Attack").desc,col("Sp_Atk").desc)).alias("rank"))
      .filter(col("rank") === 1 )
      .drop(col("rank"))

    var agg_legend_poke = pokemon.filter(col("Legendary") === true and col("Type_2") === "Flying")
      .groupBy("Type_1").agg(count("Total").alias("Total_Number"),mean("Total").alias("Average_Power"))
      .orderBy(col("Total_Number").desc)

    var special_criteria_poke = pokemon.filter(col("Generation").isin(1,2,4,5)
      and col("HP") > 70
      and col("Attack") > 100
      and col("Defense") < 80
      and col("Speed").between(50,100)).withColumn("Name",
      trim(when(col("Name").like("% %"),col("Name").substr(lit(1), instr(col("Name"), " ")))
        .otherwise(col("Name"))))
      .orderBy(col("Total").desc)



    max_attack_per_type.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save("/SparkScalaCourse/SparkScalaCourse/output_data/pokemon/max_attack_per_type")

    agg_legend_poke.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save("/SparkScalaCourse/SparkScalaCourse/output_data/pokemon/agg_legend_poke")

    special_criteria_poke.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save("/SparkScalaCourse/SparkScalaCourse/output_data/pokemon/special_criteria_poke")



    spark.stop()
  }
}