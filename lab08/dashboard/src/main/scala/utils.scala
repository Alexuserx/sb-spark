import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import java.net.{URL, URLDecoder}
import scala.util.{Failure, Success, Try}

object utils {
  def get_pipeline(): Pipeline = {
    val stringIndexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")

    val countVectorizer = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")
      .setVocabSize(10000)

    val logisticRegression = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val pipeline = new Pipeline()
      .setStages(
        Array(
          stringIndexer,
          countVectorizer,
          logisticRegression
        )
      )

    pipeline
  }

  val urlDecoderUDF: UserDefinedFunction = udf((urls: Seq[Row]) => {
    Try {
      urls.map(r => new URL(URLDecoder.decode(r.getAs("url"), "UTF-8")).getHost.stripPrefix("www."))
    } match {
      case Success(urls) => urls
      case Failure(_) => Seq("")
    }
  })

}
