import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.Try

object train {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("lab07s")
      .config("spark.driver.cores", "4")
      .config("spark.driver.memory", "4G")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val train_path: String = Try {spark.sparkContext.getConf.get("spark.model.train_path")}
      .getOrElse("/labs/laba07/laba07.json")
    val model_path: String = Try {spark.sparkContext.getConf.get("spark.model.model_path")}
      .getOrElse("/tmp/pipeline_yurchenko")
    println(s"<<< CONF >>> => \n\ttrain_path=$train_path,\n\tmodel_path=$model_path")

    val trainingDF = spark
      .read
      .format("json")
      .load(train_path)

    val trainingParsedDF = trainingDF
      .withColumn("domains", utils.urlDecoderUDF(col("visits")).cast("array<string>"))
      .drop(col("visits"))
    println("<<< PARSED DATA >>>")

    val pipeline = utils.get_pipeline()
    val model = pipeline.fit(trainingParsedDF)
    println("<<< FITTED MODEL >>>")

    model.write.overwrite().save(model_path)
    println("<<< SAVED MODEL >>>")
  }
}
