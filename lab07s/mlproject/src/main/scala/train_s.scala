import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.util.Try

object train_s {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("lab07s")
      .config("spark.driver.cores", "4")
      .config("spark.driver.memory", "4G")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val input_path: String = Try {spark.sparkContext.getConf.get("spark.model.input_path")}
      .getOrElse("/labs/laba07/laba07.json")
    val model_path: String = Try {spark.sparkContext.getConf.get("spark.model.model_path")}
      .getOrElse("/tmp/pipeline_yurchenko")
    val count_by_class: Long = Try {spark.sparkContext.getConf.get("spark.model.count_by_class")}
      .getOrElse("1000").toLong
    println(s"<<< CONF >>> Params: " +
      s"\n\tinput_path=$input_path," +
      s"\n\tmodel_path=$model_path," +
      s"\n\tcount_by_class=$count_by_class")

    // ------------------------ Read data ------------------------
    val trainingFullDF = spark
      .read
      .format("json")
      .load(input_path)
    println("<<< Read train data >>>")

    val winSpec = Window.partitionBy(col("gender_age")).orderBy(md5(col("uid")))
    val training = trainingFullDF
      .withColumn("label_rnk", row_number().over(winSpec))
      .filter(col("label_rnk") < count_by_class)
    println("<<< Preprocessed train data >>>")

    // ------------------------ Pipeline -------------------------
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
       .setLabelCol("label")
       .setFeaturesCol("features")

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
    println("<<< Defined pipeline >>>")

    val model = pipeline.fit(training)
    println("<<< Fitted pipeline >>>")

    model.write.overwrite().save(model_path)
    println("<<< Saved model >>>")

    spark.stop()
  }

}
