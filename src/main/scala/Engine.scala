import SimilarArticles.logger
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{col, desc, sum, udf, size => arraySize}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.io.StdIn

class Engine {


  val matchAttributesHelperUDF: (Map[String, String]) => UserDefinedFunction = example =>
    udf[Seq[String], Row] { row =>
      val parsedRow: Map[String, String] = row.getValuesMap[String](row.schema.names)

      val matchingAttributes =
        parsedRow.filter { case (key, value) => example.get(key).contains(value) }

      matchingAttributes.keys.toSeq.sorted
    }


  /**
   * Initialize Engine
   * request SKU and product items to be displayed
   *
   * @param sparkSession
   * @return
   */

  def initialize()(implicit sparkSession: SparkSession): DataFrame = {

    val conf = new Configuration()
    logger.info(conf.summary)


    val jsonData = conf.jsonData()
    val sku = StdIn.readLine("Please enter valid SKU (Product Id): ")
    val numberOfSimilarProducts = StdIn.readLine("Please enter valid number of similar product items to be displayed: ").toInt
    val items: DataFrame = loadItems(jsonData)

    val givenItemAttributes: Map[String, String] = getAttributes(sku, items)
      .getOrElse {
        sparkSession.stop()
        throw new IllegalArgumentException(s"SKU (Product Id) $sku not found. Please enter valid product id.")
      }

    val matchAttributesUDF: UserDefinedFunction = matchAttributesHelperUDF(givenItemAttributes)

    val similarProductList: DataFrame = items
      .filter(col(ArticleData.sku) =!= sku)
      .withColumn(ArticleData.matchingAttributes, matchAttributesUDF(col(ArticleData.attributes)))
      .withColumn(ArticleData.matchingCount, arraySize(col(ArticleData.matchingAttributes)))

    getSimilarProductList(similarProductList, numberOfSimilarProducts)
  }

  /**
   * @param path : Load jsonData from path
   * @param sparkSession
   * @return read jsonData
   */

  def loadItems(path: String)(implicit sparkSession: SparkSession): DataFrame =
    sparkSession.read.json(path)

  /**
   *
   * @param sku   : given product id
   * @param items : content of jsonData
   * @return atrributes of the given sku
   */

  def getAttributes(sku: String, items: DataFrame): Option[Map[String, String]] = {


    val extractedRow = items
      .filter(col(ArticleData.sku) === sku)
      .select(ArticleData.attributes)
      .collect()
      .headOption
      .map(_.getStruct(0))

    extractedRow.map(row => row.getValuesMap[String](row.schema.names))

  }

  /**
   *
   * @param similarProductList      : List of similar products
   * @param numberOfSimilarProducts count of similar produts
   * @param sparkSession
   * @return: sorted similar product list with attributes
   */

  def getSimilarProductList(similarProductList: DataFrame, numberOfSimilarProducts: Int)
                           (implicit sparkSession: SparkSession): DataFrame = {

    val matchingCounts: Array[MatchingCount] = calcMatchingAttributeCounts(similarProductList)

    val selectAllItemsUntilCount: Option[MatchingCount] =
      matchingCounts
        .sortBy(_.count)
        .find(_.cumsum <= numberOfSimilarProducts)

    val numberOfAdditionalItemsNeeded: Long =
      Math.min(numberOfSimilarProducts, similarProductList.count()) -
        selectAllItemsUntilCount.map(_.cumsum).getOrElse(0L)

    val countWhereSelectionIsNeeded: Long =
      matchingCounts
        .sortBy(-_.count)
        .find(_.cumsum > numberOfSimilarProducts)
        .getOrElse(matchingCounts.maxBy(_.count))
        .count


    val itemsWithoutSelection =
      selectAllItemsUntilCount
        .map { matchingCount =>
          logger.info(s"Selecting ${matchingCount.cumsum} items with" +
            s" ${matchingCount.count} matching attributes.")

          similarProductList.filter(col(ArticleData.matchingCount) >= matchingCount.count)
        }
        .getOrElse(
          sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], similarProductList.schema)
        )

    logger.info(s"Selecting $numberOfAdditionalItemsNeeded additional items " +
      s"with $countWhereSelectionIsNeeded matching attributes based on given sorting criterion.")

    val unsortedRecommendations = if (numberOfAdditionalItemsNeeded > 0) {
      val additionalItems = similarProductList
        .filter(col(ArticleData.matchingCount) === countWhereSelectionIsNeeded)
        .orderBy(ArticleData.matchingAttributes)
        .limit(numberOfAdditionalItemsNeeded.toInt)

      itemsWithoutSelection.union(additionalItems)
    } else
      itemsWithoutSelection

    unsortedRecommendations
      .orderBy(desc(ArticleData.matchingCount), col(ArticleData.matchingAttributes))
  }

  /**
   *
   * @param similarProductList : List of similar products
   * @param sparkSession
   * @return: count of similar products with matched attributes
   */
  def calcMatchingAttributeCounts(similarProductList: DataFrame)
                                 (implicit sparkSession: SparkSession): Array[MatchingCount] = {
    val window = Window
      .orderBy(desc(ArticleData.matchingCount))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    import sparkSession.implicits._

    val candidateStatistics = similarProductList
      .groupBy(ArticleData.matchingCount)
      .count()
      .withColumn("cumsum", sum("count").over(window))
      .select(col(ArticleData.matchingCount).as("count"), col("cumsum"))

    candidateStatistics
      .as[MatchingCount]
      .collect()
  }

}
