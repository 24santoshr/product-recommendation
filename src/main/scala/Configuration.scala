import org.rogach.scallop.ScallopConf

/**
 * configuring jsonData
 */
class Configuration() extends ScallopConf {

  val jsonData = opt[String](name = "jsonData", default = Some("test-data-for-spark.json"))

  verify()
}

