import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{array_distinct, col, collect_list, flatten, from_unixtime, regexp_extract, sort_array, unix_timestamp, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object Spark2Application {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Spark 2 Recruitment Challenge")
      .getOrCreate()

    //Part 1
    val schema_df_1 = StructType(
      StructField("App", StringType, true) ::
        StructField("Translated_Review", StringType, true) ::
        StructField("Sentiment", StringType, true) ::
        StructField("Sentiment_Polarity", DoubleType, false) ::
        StructField("Sentiment_Subjectivity", DoubleType, false) :: Nil
    )

    var df = spark.read.options(Map("header"->"true", "escape"->"\"")).schema(schema_df_1).csv("src/main/resources/googleplaystore_user_reviews.csv").na.fill(0)

    val df_1 = df.groupBy("App").avg("Sentiment_Polarity").withColumnRenamed("avg(Sentiment_Polarity)","Average_Sentiment_Polarity")

    df_1.printSchema()

    df_1.show()
    //Part 2
    val schema_df_2 = StructType(
      StructField("App", StringType, false) ::
        StructField("Category", StringType, false) ::
        StructField("Rating", DoubleType, false) ::
        StructField("Reviews", IntegerType, false) ::
        StructField("Size", StringType, false) ::
        StructField("Installs", StringType, false) ::
        StructField("Type", StringType, false) ::
        StructField("Price", StringType, false) ::
        StructField("Content Rating", StringType, false) ::
        StructField("Genres", StringType, false) ::
        StructField("Last Updated", StringType, false) ::
        StructField("Current Ver", StringType, false) ::
        StructField("Android Ver", StringType, false) :: Nil
    )

    df = spark.read.options(Map("header"->"true")).schema(schema_df_2).csv("src/main/resources/googleplaystore.csv").na.fill(0)

    val df_2 = df.filter(df("Rating")>4.0).sort(col("Rating").desc)

    df_2.printSchema()

    df_2.show()

    /*df_2.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header",
      "true").options(Map("header"->"true", "delimiter"->"§")).csv("src/main/resoures/best_apps.csv")*/
  }
}
