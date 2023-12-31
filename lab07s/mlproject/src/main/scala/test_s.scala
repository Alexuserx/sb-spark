import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.{IndexToString, StringIndexerModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import scala.util.Try
import sys.process._

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
            StructField("url", StringType, nullable = false),
            StructField("timestamp", StringType, nullable = false)
          )
        ), containsNull = false
      ), nullable = false
      )
    )
  )

  def main(args: Array[String]): Unit = {

    try {
      println("hdfs dfs -rm -r /tmp/chk_yurchenko".!!)
    } catch {
      case e: Throwable => println(s"Is Empty ${e.toString}")
    }

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

    val testDF = spark
      .readStream
      .format("kafka")
      .options(kafkaInputParams)
      .load
    println("<<< Loaded data >>>")

    val testParsedDF = testDF
      .select(col("value").cast(StringType))
      .withColumn("jsonData", from_json(col("value"), valueSchema))
      .select(col("jsonData.*"))
    println("<<< Parsed data >>>")

    val model = PipelineModel.load(model_path)
    println("<<< Loaded  PipelineModel[... SklearnEstimatorModel ...] >>>")

    // ----- you have to know index of StringIndexerModel in pipeline -----
     val indexToString = new IndexToString()
       .setInputCol("prediction")
       .setOutputCol("gender_age")
       .setLabels(model.stages(0).asInstanceOf[StringIndexerModel].labels)

    val writeQuery = testParsedDF
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val predictionsDF = model.transform(batchDF)
        val resultDF = indexToString.transform(predictionsDF)
          .select(col("uid"), col("gender_age"))
          .toJSON
          .withColumn("key", lit(null).cast(StringType))
        resultDF
          .write
          .format("kafka")
          .options(kafkaOutputParams)
          .save()
      }
//      .format("kafka")
//      .outputMode("update")
      .trigger(Trigger.ProcessingTime("5 seconds"))
//      .options(kafkaOutputParams)
      .start

    writeQuery.awaitTermination()

    spark.stop()

  }

}
