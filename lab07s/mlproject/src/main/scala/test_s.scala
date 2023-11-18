import org.apache.hadoop.fs.Path
import org.apache.spark.ml.feature.SklearnEstimatorModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import scala.sys.process._
import scala.util.Try

object test_s {
  private val kafkaInputParams: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> "aleksandr_yurchenko"
  )

  private val kafkaOutputParams: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "checkpointLocation" -> "/tmp/chk_yurchenko",
    "topic" -> "aleksandr_yurchenko_lab07s_out"
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

    println("hdfs dfs -rm -r /tmp/chk_yurchenko".!!)

    val spark: SparkSession = SparkSession.builder()
      .appName("lab07s")
      .config("spark.driver.cores", "4")
      .config("spark.driver.memory", "4G")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val model_path: String = Try {spark.sparkContext.getConf.get("spark.model.model_path")}
      .getOrElse("/tmp/pipeline_yurchenko")
    println(s"<<< CONF >>> Params: " +
      s"\n\tmodel_path=$model_path")

    val sdfInput = spark
      .readStream
      .format("kafka")
      .options(kafkaInputParams)
      .load

    val testDF = sdfInput
      .select(col("value").cast(StringType))
      .withColumn("jsonData", from_json(col("value"), valueSchema))
      .select(col("jsonData.*"))
    println("<<< Loaded data >>>")

    val testParsedDF = testDF
      .withColumn("gender_age", lit("M:25-34").cast(StringType))
    println("<<< Parsed data >>>")

    val model = SklearnEstimatorModel.load(model_path)
    println("<<< Loaded  SklearnEstimatorModel >>>")

    val resultDF = model.transform(testParsedDF)
      .select(col("uid"), col("original_label").alias("gender_age"))
      .toJSON
      .withColumn("key", lit(null).cast(StringType))
    println("<<< Applied SklearnEstimatorModel >>>")

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