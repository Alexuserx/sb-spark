import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object features extends java.io.Serializable {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("laba06")
      .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val logs: DataFrame = spark.read.json("hdfs:///labs/laba03/weblogs.json")
    println("<<< read logs >>>")
    val users_items_df = spark.read.parquet("/user/aleksandr.yurchenko/users-items/20200429")
    println("<<< read users x items >>>")

    val logs_df: DataFrame = logs
      .withColumn("visit", explode(col("visits")))
      .withColumn("host", lower(callUDF("parse_url", col("visit.url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))
      .filter(col("domain") =!= lit("null"))
      .select(
        col("uid"),
        col("domain"),
        from_unixtime(col("visit.timestamp") / 1000).alias("timestamp"))
      .withColumn("week", date_format(col("timestamp"), "E"))
      .withColumn("web_week", concat(lit("web_day_"), lower(col("week"))))
      .withColumn("hour", date_format(col("timestamp"), "HH").cast("int"))
      .withColumn("web_hour", concat(lit("web_hour_"), col("hour").cast("string")))
    println("<<< prepared logs >>>")

    val pivoted_week_df = logs_df.groupBy("uid").pivot("web_week").count()
    val pivoted_hour_df = logs_df.groupBy("uid").pivot("web_hour").count()
    println("<<< pivoted days and hours >>>")

    val fractions_df = logs_df.groupBy("uid")
      .agg(
        (sum(when(col("hour") >= 9 && col("hour") < 18, lit(1))
          .otherwise(lit(0))) / count("hour")).alias("web_fraction_work_hours"),
        (sum(when(col("hour") >= 18 && col("hour") < 24, lit(1))
          .otherwise(lit(0))) / count("hour")).alias("web_fraction_evening_hours"))
    println("<<< calculated fractions >>>")

    val top_domains_1000 = logs_df.groupBy("domain").count()
      .withColumn("rnk", rank().over(Window.orderBy(col("count").desc, col("domain").asc)))
      .filter("rnk <= 1000").select("domain").cache()
    val top_domains_1000_list = top_domains_1000.orderBy("domain").rdd.map(_.getString(0)).collect()
    println("<<< got top 1000 domians >>>")

    val pivoted_domains_df = logs_df.join(top_domains_1000, Seq("domain"), "inner") // join не обязательно
      .groupBy("uid").pivot("domain").count().na.fill(0)
      .select(col("uid"),
        array(top_domains_1000_list.map(c => col(s"`$c`")): _*).cast("array<int>").alias("domain_features"))
    println("<<< pivoted domains >>> ")

    val final_df = logs_df.select("uid").distinct()
      .join(pivoted_week_df, Seq("uid"), "left")
      .join(pivoted_hour_df, Seq("uid"), "left")
      .join(fractions_df, Seq("uid"), "left")
      .join(pivoted_domains_df, Seq("uid"), "left")
      .join(users_items_df, Seq("uid"), "left")
      .na.fill(0)

    final_df
      .write
      .format("parquet")
      .mode("overwrite")
      .save("/user/aleksandr.yurchenko/features")

    println("<<< DONE >>>")
  }
}