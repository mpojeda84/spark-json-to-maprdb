package com.mpojeda84.mapr.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.mapr.db.spark.sql._
import org.apache.spark.sql.types.StringType




object Application {

  def main(args: Array[String]): Unit = {


    val argsConfiguration = Configuration.parse(args)

    val spark = SparkSession.builder.appName(argsConfiguration.appName).getOrCreate

    println("### Running ###")


    val fileName = argsConfiguration.jsonFile
    val tableName = argsConfiguration.tableName

    println(fileName)
    println(tableName)

    spark
      .read
      .json(fileName)
      .withColumn("_id",  monotonically_increasing_id.cast(StringType))
      .saveToMapRDB(tableName)

    spark.close()
    println("### Done! ###")

  }

}



