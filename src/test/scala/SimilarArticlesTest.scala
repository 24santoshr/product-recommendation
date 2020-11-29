import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array_contains, col}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, MustMatchers, OptionValues}


class SimilarArticlesTest extends FlatSpec with MustMatchers with OptionValues {

  behavior of "SimilarArticles"

  private val engine: Engine = new Engine()


  val conf =
    new SparkConf()
      .setAppName("SimilarArticlesTest")
      .setMaster("local[*]")

  implicit val sparkSession = SparkSession.builder.config(conf).getOrCreate()

  val jsonData = getClass.getResource("items.json").getPath

  it should "load items correctly from the json data file" in {

    val items = engine.loadItems(jsonData)
    items.columns must contain theSameElementsAs Seq("attributes", "sku")
    items.count() mustBe 10
  }

  it should "extract item correctly from the json data file" in {

    val items = engine.loadItems(jsonData)
    val extractedItem = engine.getAttributes("sku-1", items)
    extractedItem.value mustBe Map("a" -> "a", "b" -> "b", "c" -> "c")
  }

  it should "match attributes of items correctly" in {

    val items = engine.loadItems(jsonData)
    val toBeMatchedAttributes = Map("a" -> "a", "b" -> "b", "c" -> "c")

    val matchAttributesUDF: UserDefinedFunction =
      engine.matchAttributesHelperUDF(toBeMatchedAttributes)

    val candidates: DataFrame = items
      .withColumn("matchingAttributes", matchAttributesUDF(col("attributes")))

    val actualItemsContainingAttributeA = candidates
      .filter(array_contains(col("matchingAttributes"), "a"))
      .select("sku")
      .collect()
      .map(_.getAs[String]("sku"))

    val expectedItemsContainingAttributeA =
      Seq("sku-1", "sku-2", "sku-3", "sku-4", "sku-5", "sku-6")

    actualItemsContainingAttributeA must contain theSameElementsAs expectedItemsContainingAttributeA

    val actualAttributes = candidates
      .filter(col("sku") === "sku-1")
      .select("matchingAttributes")
      .first()
      .getSeq[String](0)

    actualAttributes must contain theSameElementsAs Seq("a", "b", "c")
  }
}
