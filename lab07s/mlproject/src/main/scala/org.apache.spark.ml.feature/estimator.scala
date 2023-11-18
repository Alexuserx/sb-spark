package org.apache.spark.ml.feature

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.SklearnEstimatorModel.SklearnEstimatorModelWriter
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

class SklearnEstimator(override val uid: String) extends Estimator[SklearnEstimatorModel]
  with DefaultParamsWritable
{
  def this() = this(Identifiable.randomUID("SklearnEstimator"))

  final val labelCol = new Param[String](this, "labelCol", "The label column")
  final val featuresCol = new Param[String](this, "featuresCol", "The feature column");

  def setLabelCol(value: String): this.type = set(labelCol, value)
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  override def fit(dataset: Dataset[_]): SklearnEstimatorModel = {
    println("<<< Start method  SklearnEstimator.SklearnEstimatorModel() >>>")
    // Внутри данного метода необходимо вызывать обучение модели при помощи train.py. Используйте для этого rdd.pipe().
    // Файл train.py будет возвращать сериализованную модель в формате base64.
    // Данный метод fit возвращает SklearnEstimatorModel, поэтому инициализируйте данный объект, где в качестве параметра будет приниматься модель в формате base64.
    // ------------- так как внутри train.py названия колонок захардкожены, то не смысла их устанавливать --------------
    val pipedRDD = dataset.repartition(1).select($(featuresCol), $(labelCol)).toJSON.rdd.pipe("./train.py")
    println("<<< Successfully created DAG for pipedRDD >>>")
    val model = pipedRDD.collect()(0)
    println(s"<<< File ./train.py executed successfully >>>")
    new SklearnEstimatorModel(uid = uid, model = model)
  }

  override def copy(extra: ParamMap): SklearnEstimator = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

}

object SklearnEstimator extends DefaultParamsReadable[SklearnEstimator] {
  override def load(path: String): SklearnEstimator = super.load(path)
}


class SklearnEstimatorModel(override val uid: String, val model: String) extends Model[SklearnEstimatorModel]
  with MLWritable
{
  //как видно выше, для инициализации объекта данного класса в качестве одного из параметров конструктора является String-переменная model, это и есть модель в формате base64, которая была возвращена из train.py
  override def copy(extra: ParamMap): SklearnEstimatorModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    println("<<< Start method  SklearnEstimatorModel.transform() >>>")
    // Внутри данного метода необходимо вызывать test.py для получения предсказаний. Используйте для этого rdd.pipe().
    // Внутри test.py используется обученная модель, которая хранится в переменной `model`. Поэтому перед вызовом rdd.pipe() необходимо записать данное значение в файл и добавить его в spark-сессию при помощи sparkSession.sparkContext.addFile.
    // Данный метод возвращает DataFrame, поэтому полученные предсказания необходимо корректно преобразовать в DF.

    // ------------------- Сохраняем [локально] не сам класс модели, а только файл для python крипта ------------------
    Files.write(Paths.get("lab07.model"), model.getBytes(StandardCharsets.UTF_8))
    println("<<< Saved model to local file system >>>")
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.addFile("lab07.model")
    println("<<< Added lab07.model to Spark Context >>>")
    val pipedRDD: RDD[String] = dataset.rdd.pipe("./test.py")
    println("<<< Successfully created DAG for pipedRDD >>>")

    // ------------------- Изменить rdd, чтобы сразу создать датафрейм с нужной схемой [без кастов] ------------------
    val predsRDD = dataset.repartition(1).rdd.zip(pipedRDD).map(r => Row.fromSeq(Seq(r._1) ++ Seq(r._2)))
    println(s"<<< File ./test.py executed successfully >>>")
//    pipedRDD.map(r => r.stripPrefix("[").stripSuffix("]").split(",").map(_.toDouble)
//    val predsRDD: RDD[String] = dataset.repartition(1).rdd.zip(pipedRDD).map(r => Row.fromSeq(r._1.toSeq ++ Seq(r._2)))
    val outputFields = dataset.schema.fields :+ StructField("prediction", StringType, nullable = true)
    spark.createDataFrame(predsRDD, StructType(outputFields))
      .withColumn("prediction", from_json(col("prediction"), lit("array<double>")))
  }

  override def transformSchema(schema: StructType): StructType = {
    // Определение выходной схемы данных
    if (schema.fieldNames.contains("prediction")) {
      throw new IllegalArgumentException(s"Output column ${"prediction"} already exists.")
    }
    val outputFields = schema.fields :+
      StructField("prediction", ArrayType(DoubleType), nullable = false)
    StructType(outputFields)
  }

  override def write: MLWriter = new SklearnEstimatorModelWriter(this)
}


object SklearnEstimatorModel extends MLReadable[SklearnEstimatorModel] {
  private[SklearnEstimatorModel]
  class SklearnEstimatorModelWriter(instance: SklearnEstimatorModel) extends MLWriter {

    private case class Data(model: String)

    override protected def saveImpl(path: String): Unit = {
      // В данном методе сохраняется значение модели в формате base64 на hdfs
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.model)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class SklearnEstimatorModelReader extends MLReader[SklearnEstimatorModel] {

    private val className = classOf[SklearnEstimatorModel].getName

    override def load(path: String): SklearnEstimatorModel = {
      // В данном методе считывается значение модели в формате base64 из hdfs
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("model")
        .head()
      val modelStr = data.getAs[String](0)
      val model = new SklearnEstimatorModel(metadata.uid, modelStr)
      metadata.getAndSetParams(model)
      model
    }
  }

  override def read: MLReader[SklearnEstimatorModel] = new SklearnEstimatorModelReader

  override def load(path: String): SklearnEstimatorModel = super.load(path)
}