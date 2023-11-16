import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession

import scala.util.Try

object train_s {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("laba07s")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val input_path: String = Try {spark.sparkContext.getConf.get("spark.model.input_path")}
      .getOrElse("/labs/laba07/laba07.json")
    val model_path: String = Try {spark.sparkContext.getConf.get("spark.model.model_path")}
      .getOrElse("/tmp/pipeline_yurchenko")
    println(s"<<< CONF >>> => input_path=$input_path,model_path=$model_path")

    val url2DomainTransformer = new Url2DomainTransformer()
      .setInputCol("visits")
      .setOutputCol("domains")

    val countVectorizer = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val stringIndexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")

    val sklearnEstimator = new SklearnEstimator()

    val indexToString = new IndexToString()
      .setInputCol("label")
      .setOutputCol("original_label")

    val pipeline = new Pipeline()
      .setStages(
        Array(
          url2DomainTransformer,
          countVectorizer,
          stringIndexer,
          sklearnEstimator,
          indexToString
        )
      )

    val trainingDF = spark
      .read
      .format("json")
      .load(input_path)

    val model = pipeline.fit(trainingDF)
    println("<<< FITTED MODEL >>>")

    model.write.overwrite().save(model_path)
    println("<<< SAVED MODEL >>>")

  }
}
