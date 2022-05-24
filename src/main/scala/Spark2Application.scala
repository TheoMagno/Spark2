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
  }
}
