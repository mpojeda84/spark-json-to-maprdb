package com.mpojeda84.mapr.scala

;

case class Configuration(jsonFile: String, tableName: String, appName: String)

object Configuration {

  def parse(args: Seq[String]): Configuration = parser.parse(args, Configuration.default).get

  def default: Configuration = DefaultConfiguration

  object DefaultConfiguration extends Configuration(
    "path/to/json",
    "/path/to/table",
    "App Name"
  )

  private val parser = new scopt.OptionParser[Configuration]("App Name") {
    head("App Name")

    opt[String]('f', "file")
      .action((p, config) => config.copy(jsonFile = p))
      .maxOccurs(1)
      .text("Json file (HDFS)")

    opt[String]('t', "table")
      .action((t, config) => config.copy(tableName = t))
      .maxOccurs(1)
      .text("MapR-DB table name to write stats to")

    opt[String]('n', "name")
      .action((n, config) => config.copy(appName = n))
      .text("Spark application name override")
  }
}