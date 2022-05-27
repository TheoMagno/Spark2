import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{array_distinct, avg, col, collect_list, count, explode, first, flatten, from_unixtime, lit, regexp_extract, sort_array, unix_timestamp}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object Spark2Application {
  def main(args: Array[String]) = {
    //Create Spark session
    val spark = SparkSession.builder()
      .master("local")
      .appName("Spark 2 Recruitment Challenge")
      .getOrCreate()

    //Part 1
    //Dataframe 1 Structure
    val schema_df_1 = StructType(
      StructField("App", StringType, true) ::
        StructField("Translated_Review", StringType, true) ::
        StructField("Sentiment", StringType, true) ::
        StructField("Sentiment_Polarity", DoubleType, false) ::
        StructField("Sentiment_Subjectivity", DoubleType, false) :: Nil
    )
    //Read csv file with schema
    var df = spark.read.options(Map("header"->"true", "escape"->"\"")).schema(schema_df_1).csv("src/main/resources/googleplaystore_user_reviews.csv").na.fill(0)
    //Group by app name making average of sentiment polarity
    val df_1 = df.groupBy("App").avg("Sentiment_Polarity").withColumnRenamed("avg(Sentiment_Polarity)","Average_Sentiment_Polarity")

    df_1.printSchema()

    df_1.show()
    //Part 2
    //Dataframe Structure to import the csv
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
    //Read csv file with schema
    df = spark.read.options(Map("header"->"true")).schema(schema_df_2).csv("src/main/resources/googleplaystore.csv").na.fill(0)
    //Filter dataframe by showing only apps with Rating greater than 4.0, sorted in descending order
    val df_2 = df.filter(df("Rating")>4.0).sort(col("Rating").desc)

    //Print schema to show that the structure is being used
    //df_2.printSchema()

    //Show dataframe to check if is correct before writing
    //df_2.show()

    //Write dataframe to csv file with delimiter §
    df_2.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header",
      "true").options(Map("header"->"true", "delimiter"->"§")).csv("src/main/resources/best_apps")
    //Part 3
    //Dataframe structure to import the csv
    val schema_df_3 = StructType(
      StructField("App", StringType, false) ::
        StructField("Categories", StringType, false) ::
        StructField("Rating", DoubleType, false) ::
        StructField("Reviews", LongType, false) ::
        StructField("Size", StringType, false) ::
        StructField("Installs", StringType, false) ::
        StructField("Type", StringType, false) ::
        StructField("Price", DoubleType, false) ::
        StructField("Content_Rating", StringType, false) ::
        StructField("Genres", StringType, false) ::
        StructField("Last_Updated", StringType, false) ::
        StructField("Current_Version", StringType, false) ::
        StructField("Minimum_Android_Version", StringType, false) :: Nil
    )
    //Read csv using schema
    df = spark.read.options(Map("header"->"true")).schema(schema_df_3).csv("src/main/resources/googleplaystore.csv").na.fill(0)
    //Temporary dataframe that changes Categories column to array of categories, groups by app name and aggregates using the maximum rating and the list of categories, removing duplicates
    val temp_1 =df.withColumn("Categories",  functions.split(col("Categories")," ").as("Categories"))
      .groupBy(col("App"))
      .agg(array_distinct(flatten(collect_list(col("Categories")))).alias("Categories"), functions.max("Rating").as("Rating"))
      .withColumn("Categories", array_distinct(sort_array(col("Categories"))))

    //Temporary dataframe that changes the other fields (cast Size to Double, convert Price to euros, cast Last_Updated to Date, split Genres to Array of genres) and drops Categories and Rating
    val temp_2 = df.withColumn("Size",regexp_extract(col("Size"), "[0-9]+", 0).cast("Double"))
      .withColumn("Price",col("Price")*0.9)
      .withColumn("Last_Updated", from_unixtime(unix_timestamp(col("Last_Updated"), "MMMMM dd, yyyy")).cast("Date"))
      .withColumn("Genres", functions.split(col("Genres"),";").as("Genres"))
      .drop("Categories", "Rating")
      .drop("Categories", "Rating")

    //Join the two temporary dataframes, dropping duplicates, and produces Dataframe 3
    val df_3 = temp_1.join(temp_2, "App").dropDuplicates("App")

    //Print schema to show that the structure is being used
    //df_3.printSchema()

    //Show dataframe to check if the dataframe is correct
    df_3.show()
    //Part 4
    //Join df_1 with df_3 using column App
    val temp = df_3.join(df_1, "App")

    //Write to parquet file with gzip compression
    temp.write.mode("overwrite").options("compression"->"gzip", "header"->"true").parquet("src/main/resources/outputs/googleplaystore_cleaned")
    //Part 5
    val df_4 = temp.select(col("App"),explode(col("Genres")).alias("Genre")).join(temp, "App")
      .groupBy("Genre").agg(count(lit(1)).alias("Count"), avg(col("Rating")).alias("Average_Rating"), avg(col("Average_Sentiment_Polarity")).alias("Average_Sentiment_Polarity"))

    df_4.show()
    //Write to parquet file with gzip compression
    df_4.write.mode("overwrite").options("compression"->"gzip", "header"->"true").parquet("src/main/resources/outputs/googleplaystore_metrics")
  }
}
