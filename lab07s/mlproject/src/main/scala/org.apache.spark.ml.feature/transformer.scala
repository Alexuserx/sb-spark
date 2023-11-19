package org.apache.spark.ml.feature

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.net.{URL, URLDecoder}
import scala.util.{Failure, Success, Try}


class Url2DomainTransformer(override val uid: String) extends UnaryTransformer[Seq[Row], Seq[String], Url2DomainTransformer]
  with DefaultParamsWritable
{
  def this() = this(Identifiable.randomUID("org.apache.spark.ml.feature.Url2DomainTransformer"))

  override def createTransformFunc: Seq[Row] => Seq[String] = (urls: Seq[Row]) => {
    Try {
      urls.map(r => new URL(URLDecoder.decode(r.getAs("url"), "UTF-8")).getHost.stripPrefix("www."))
    } match {
      case Success(urls) => urls
      case Failure(_) => Seq("")
    }
  }
  override def outputDataType: DataType = ArrayType(StringType)

}

object Url2DomainTransformer extends DefaultParamsReadable[Url2DomainTransformer] {
  override def load(path: String): Url2DomainTransformer = super.load(path)
}