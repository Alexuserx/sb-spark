import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.postgresql.Driver

import java.sql.{Connection, DriverManager, Statement}
import java.net.{URL, URLDecoder}
import scala.util.Try

object data_mart {
  private val cassandraHost = "10.0.0.31"
  private val cassandraPort = "9042"

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .config("spark.cassandra.connection.host", cassandraHost)
      .config("spark.cassandra.connection.port", cassandraPort)
      .getOrCreate()

    val main_class = new data_mart()
    main_class.run(spark)
  }
}

class data_mart extends Serializable {
  private val postgresAddress = "10.0.0.31:5432"
  private val userName = "aleksandr_yurchenko"
  private val password = "cbvIX12i"
  private val postgresSourceDbName = "labdata"
  private val postgresDbName = "aleksandr_yurchenko"
  private val cassandraKeySpace = "labdata"
  private val cassandraTable = "clients"
  private val elasticSearchPort = "9200"
  private val elasticSearchNode = "10.0.0.31"


  private def getAgeCategory(): Column = {
    when(col("age") >= 18 && col("age") <= 24, lit("18-24"))
      .when(col("age") >= 25 && col("age") <= 34, lit("25-34"))
      .when(col("age") >= 35 && col("age") <= 44, lit("35-44"))
      .when(col("age") >= 45 && col("age") <= 54, lit("45-54"))
      .when(col("age") >= 55, lit(">=55"))
  }

  private def decodeUrlAndGetDomain: UserDefinedFunction = udf((url: String) => {
    Try {
      new URL(URLDecoder.decode(url, "UTF-8")).getHost.stripPrefix("www.")
    }.getOrElse("")
  })

  private def grantTable(): Unit = {
    val driverClass: Class[Driver] = classOf[org.postgresql.Driver]
    val driver: Any = Class.forName("org.postgresql.Driver").newInstance()
    val url = s"jdbc:postgresql://$postgresAddress/$postgresDbName?user=$userName&password=$password"
    val connection: Connection = DriverManager.getConnection(url)
    val statement: Statement = connection.createStatement()
    val bool: Boolean = statement.execute("GRANT SELECT ON clients TO labchecker2")
    connection.close()
  }

  def run(spark: SparkSession): Unit = {

    // --------------------------------------------------------------------------------
    val clients: DataFrame = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> cassandraTable, "keyspace" -> cassandraKeySpace))
      .load()

    val clientsDF: DataFrame = clients
      .withColumn("age_cat", getAgeCategory())
      .select(col("uid"), col("age_cat"), col("gender"))

    // --------------------------------------------------------------------------------
    val visits: DataFrame = spark.read
      .format("org.elasticsearch.spark.sql")
      .options(
        Map(
          "es.read.metadata" -> "true",
          "es.nodes.wan.only" -> "true",
          "es.port" -> elasticSearchPort,
          "es.nodes" -> elasticSearchNode,
          "es.net.ssl" -> "false"
        )
      )
      .load("visits")

    val visitsDF = visits
      .select(col("uid"), col("category"))
      .withColumn("category",
        concat(lit("shop_"), lower(regexp_replace(col("category"), "[ -]+", "_"))))

    val itemsCatDF = visitsDF
      .groupBy(col("uid"))
      .pivot(col("category"))
      .agg(count(lit(1)))

    // --------------------------------------------------------------------------------
    val logs: DataFrame = spark.read.json("hdfs:///labs/laba03/weblogs.json")

    val logsDF = logs
      .withColumn("visit_info", explode(col("visits")))
      .select(col("uid"), col("visit_info").getField("url").as("domain"))
      .withColumn("domain", decodeUrlAndGetDomain(col("domain")))

    // --------------------------------------------------------------------------------
    val cats: DataFrame = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://$postgresAddress/$postgresSourceDbName")
      .option("dbtable", "domain_cats")
      .option("user", userName)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .load()

    val catsDF = cats
      .withColumn("category", concat(lit("web_"), col("category")))

    val domainsCatDF = logsDF
      .join(catsDF, Seq("domain"), "inner")
      .groupBy(col("uid"))
      .pivot(col("category"))
      .agg(count(lit(1)))

    // --------------------------------------------------------------------------------
    val finalDF = clientsDF
      .join(itemsCatDF, Seq("uid"), "left")
      .join(domainsCatDF, Seq("uid"), "left")

    finalDF.show()

    finalDF.write
      .format("jdbc")
      .option("url", s"jdbc:postgresql://$postgresAddress/$postgresDbName")
      .option("dbtable", "clients")
      .option("user", userName)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .option("truncate", value = true)
      .mode("overwrite")
      .save()

    grantTable()

  }

}