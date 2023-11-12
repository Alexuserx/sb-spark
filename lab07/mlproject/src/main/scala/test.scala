import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import utils._
import sys.process._

import scala.util.Try

object test {
  private val kafkaInputParams: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> "aleksandr_yurchenko"
  )

  private val kafkaOutputParams: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> "aleksandr_yurchenko_lab07_out",
    "checkpointLocation" -> "/tmp/chk_yurchenko"
  )

  private val valueSchema: StructType = StructType(
    Seq(
      StructField("uid", StringType, nullable = false),
      StructField("visits", ArrayType(
        StructType(
          Seq(
            StructField("uid", StringType, nullable = false),
            StructField("timestamp", StringType, nullable = false)
          )
        ), containsNull = false
      ), nullable = false
      )
    )
  )

  def main(args: Array[String]): Unit = {

    "hdfs dfs -rm -r /tmp/chk_yurchenko".!!

    val spark: SparkSession = SparkSession.builder()
      .appName("laba07")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val model_path: String = Try {spark.sparkContext.getConf.get("spark.model.model_path")}
      .getOrElse("/tmp/pipeline_yurchenko")

    val sdfInput = spark
      .readStream
      .format("kafka")
      .options(kafkaInputParams)
      .load

    val testDF = sdfInput
      .select(col("value").cast(StringType))
      .withColumn("jsonData", from_json(col("value"), valueSchema))
      .select(col("jsonData.*"))
    println("<<< LOADED DATA >>>")

    val testParsedDF = testDF
      .withColumn("domains", urlDecoderUDF(col("visits")).cast("array<string>"))
      .drop(col("visits"))
      .withColumn("gender_age", lit(null).cast(StringType))
    println("<<< PARSED DATA >>>")

    val model = PipelineModel.load(model_path)
    println("<<< LOADED MODEL >>>")

    val resultDF = model.transform(testParsedDF)
      .select(col("uid"), col("original_label").alias("gender_age"))
      .toJSON
      .withColumn("key", lit(null).cast(StringType))
    println("<<< APPLIED MODEL >>>")

    val writeQuery = resultDF
      .writeStream
      .format("kafka")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .options(kafkaOutputParams)
      .start

    writeQuery.awaitTermination()

    spark.stop()

  }

}
