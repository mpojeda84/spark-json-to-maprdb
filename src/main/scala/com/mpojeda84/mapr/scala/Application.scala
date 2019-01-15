package com.mpojeda84.mapr.scala

import org.apache.spark.sql.SparkSession
import com.mapr.db.spark.sql._


object Application {

  def main(args: Array[String]): Unit = {


    val argsConfiguration = Configuration.parse(args)

    val spark = SparkSession.builder.appName(argsConfiguration.appName).getOrCreate

    println("### Running ###")


    val fileName = argsConfiguration.jsonFile
    val tableName = argsConfiguration.tableName
    val idField = argsConfiguration.idField

    println(fileName)
    println(tableName)
    println(idField)

    spark
      .read
      .json(fileName)
      .saveToMapRDB(tableName, idField, true, false)

  }
}



