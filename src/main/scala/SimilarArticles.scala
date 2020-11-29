import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Initializing spark session and running the engine
 */

object SimilarArticles {

  val logger = LoggerFactory.getLogger(this.getClass)

  private val engine = new Engine()

  def main(args: Array[String]): Unit = {

    val conf =
      new SparkConf()
        .setAppName("SimilarArticles")
        .setMaster("local[*]")

    implicit val sparkSession = SparkSession.builder.config(conf).getOrCreate()

    val getProductList = engine.initialize()
    getProductList.show(numRows = getProductList.count().toInt, truncate = false)
    sparkSession.stop()
  }
}
