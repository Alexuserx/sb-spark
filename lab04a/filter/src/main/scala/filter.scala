import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object filter extends java.io.Serializable {
  private def writeDataToHDFS(df: DataFrame, outputDirPrefix: String, path: String): Unit = {
    val pathToData = outputDirPrefix.stripSuffix("/") + "/" + path.stripPrefix("/")
    println(s"<<<<< pathToData: $pathToData >>>>>")
    val finalDF = df
      .select(
        col("value.event_type"),
        col("value.category"),
        col("value.item_id"),
        col("value.item_price"),
        col("value.uid"),
        col("value.timestamp"),
        col("value.date"),
        col("p_date")
      )

    finalDF
      .write
      .format("json")
      .mode("overwrite")
      .partitionBy("p_date")
      .save(pathToData)
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val topic: String = spark.sparkContext.getConf.get("spark.filter.topic_name")
    val offset: String = spark.sparkContext.getConf.get("spark.filter.offset")
    val prefix: String = spark.sparkContext.getConf.get("spark.filter.output_dir_prefix")
    val outputDirPrefix: String = if (prefix.contains("/")) prefix else s"/user/aleksandr.yurchenko/$prefix"

    val buyPath: String = "buy"
    val viewPath: String = "view"

    val valueSchema: StructType = StructType(
      Seq(
        StructField("event_type", StringType, nullable=false),
        StructField("category", StringType, nullable=false),
        StructField("item_id", StringType, nullable=false),
        StructField("item_price", StringType, nullable=false),
        StructField("uid", StringType, nullable=false),
        StructField("timestamp", StringType, nullable=false)
      )
    )

    val df: DataFrame = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", topic)
      .option(
        "startingOffsets",
        if (offset.contains("earliest")) offset
        else "{\"" + topic + "\":{\"0\":" + offset + "}}"
      )
      .load()
    println(s"<<<<< Loaded data from kafka topic=$topic with offset=$offset >>>>>")

    val filledWithDateDF: DataFrame = df
      .withColumn("value", from_json(col("value").cast(StringType), valueSchema))
      .withColumn("p_date",
        from_unixtime(col("value.timestamp").cast(LongType) / lit(1000L), "YYYYMMdd").cast(StringType).alias("date"))
      .select(
        struct(col("value.*"), col("p_date").alias("date")).alias("value"),
        col("p_date")
      )
      .orderBy(col("p_date").desc)

    val buyTypeDF: DataFrame = filledWithDateDF.filter(col("value.event_type") === lit("buy"))
    val viewTypeDF: DataFrame = filledWithDateDF.filter(col("value.event_type") === lit("view"))

    println(s"<<<<< Started saving data to HDFS, outputDirPrefix=$outputDirPrefix >>>>>")
    writeDataToHDFS(buyTypeDF, outputDirPrefix, buyPath)
    println(s"<<<<< Saved buyTypeDF data to HDFS buyPath=$buyPath >>>>>")
    writeDataToHDFS(viewTypeDF, outputDirPrefix, viewPath)
    println(s"<<<<< Saved viewTypeDF data to HDFS viewPath=$viewPath >>>>>")

    println("<<<<< DONE >>>>>")
  }
}