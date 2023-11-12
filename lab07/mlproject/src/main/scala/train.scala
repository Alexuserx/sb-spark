import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import utils._

import scala.util.Try

object train {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("laba07")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val input_path: String = Try {spark.sparkContext.getConf.get("spark.model.input_path")}
      .getOrElse("/labs/laba07/laba07.json")
    val model_path: String = Try {spark.sparkContext.getConf.get("spark.model.model_path")}
      .getOrElse("/tmp/pipeline_yurchenko")
    println(s"<<< CONF >>> => input_path=$input_path,model_path=$model_path")

    val trainingDF = spark
      .read
      .format("json")
      .load(input_path)

    val trainingParsedDF = trainingDF
      .withColumn("domains", urlDecoderUDF(col("visits")).cast("array<string>"))
      .drop(col("visits"))
    println("<<< PARSED DATA >>>")

    val pipeline = get_pipeline()
    val model = pipeline.fit(trainingParsedDF)
    println("<<< FITTED MODEL >>>")

    model.write.overwrite().save(model_path)
    println("<<< SAVED MODEL >>>")

    spark.stop()

  }
}
