import org.apache.spark.ml.feature.{IndexToString, StringIndexerModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.Try

object test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("lab07s")
      .config("spark.driver.cores", "4")
      .config("spark.driver.memory", "4G")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val test_path: String = Try {spark.sparkContext.getConf.get("spark.model.test_path")}
      .getOrElse("/labs/laba08/laba08.json")
    val model_path: String = Try {spark.sparkContext.getConf.get("spark.model.model_path")}
      .getOrElse("/tmp/pipeline_yurchenko")
    val es_index: String = Try {spark.sparkContext.getConf.get("spark.model.es_index")}
      .getOrElse("aleksandr_yurchenko_lab08/_doc")
    println(s"<<< CONF >>> => \n\ttest_path=$test_path,\n\tmodel_path=$model_path,\n\tes_index=$es_index")

    val testDF = spark
      .read
      .format("json")
      .load(test_path)

    val testParsedDF = testDF
      .withColumn("domains", utils.urlDecoderUDF(col("visits")).cast("array<string>"))
      .drop(col("visits"))
    println("<<< PARSED DATA >>>")

    import org.apache.spark.ml.PipelineModel

    val model = PipelineModel.load(model_path)
    println("<<< LOADED MODEL >>>")

    val indexToString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("gender_age")
      .setLabels(model.stages(0).asInstanceOf[StringIndexerModel].labels)

    val testDFWithPredictions = indexToString.transform(model.transform(testParsedDF))
      .select("uid", "gender_age", "date")

    testDFWithPredictions
      .write
      .format("org.elasticsearch.spark.sql")
      .mode("overwrite")
      .options(
        Map(
          "es.read.metadata" -> "true",
          "es.nodes.wan.only" -> "true",
          "es.port" -> "9200",
          "es.nodes" -> "10.0.0.31",
          "es.net.ssl" -> "false",
          "es.resource" -> es_index
        )
      )
      .save()
  }
}
