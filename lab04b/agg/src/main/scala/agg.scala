import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object agg {

  private val kafkaInputParams: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> "aleksandr_yurchenko"
  )

  private val kafkaOutputParams: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> "aleksandr_yurchenko_lab04b_out",
    "checkpointLocation" -> "/tmp/chk_yurchenko"
  )

  private val valueSchema: StructType = StructType(
    Seq(
      StructField("event_type", StringType, nullable = false),
      StructField("category", StringType, nullable = false),
      StructField("item_id", StringType, nullable = false),
      StructField("item_price", StringType, nullable = false),
      StructField("uid", StringType, nullable = false),
      StructField("timestamp", StringType, nullable = false)
    )
  )

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("laba04b")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val sdfInput = spark
      .readStream
      .format("kafka")
      .options(kafkaInputParams)
      .load

    val sdfValues = sdfInput
      .select(col("value").cast(StringType))
      .withColumn("jsonData", from_json(col("value"), valueSchema))
      .select(col("jsonData.*"))
      .withColumn("timestamp", (col("timestamp") / 1000).cast(TimestampType))

    val sdfAgg = sdfValues
      .groupBy(window(col("timestamp"), "1 hours"))
      .agg(
        sum(when(col("event_type") === lit("buy"), col("item_price")).otherwise(lit(0))).alias("revenue"),
        sum(when(col("uid").isNotNull, 1).otherwise(lit(0))).alias("visitors"),
        sum(when(col("event_type") === lit("buy"), lit(1)).otherwise(lit(0))).alias("purchases")
      )
      .withColumn("aov", col("revenue") / col("purchases"))
      .withColumn("start_ts", col("window").getItem("start").cast(LongType))
      .withColumn("end_ts", col("window").getItem("end").cast(LongType))
      .drop("window")

    sdfAgg
      .toJSON
      .withColumn("key", lit(null).cast(StringType))
      .writeStream
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("kafka")
      .options(kafkaOutputParams)
      .start()

  }
}
