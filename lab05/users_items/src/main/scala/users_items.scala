import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

import scala.util.Try

object users_items extends java.io.Serializable {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("laba05")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val user_name = "aleksandr.yurchenko"
    var input_dir: String = Try {spark.sparkContext.getConf.get("spark.users_items.input_dir")}
      .getOrElse(s"/user/$user_name/visits")
    input_dir = if (input_dir.contains("/")) input_dir else s"/user/$user_name/$input_dir"
    var output_dir: String = Try {spark.sparkContext.getConf.get("spark.users_items.output_dir")}
      .getOrElse(s"/user/$user_name/users-items")
    output_dir = if (output_dir.contains("/")) output_dir else s"/user/$user_name/$output_dir"
    val update: Int = Try {spark.sparkContext.getConf.get("spark.users_items.update").toInt}
      .getOrElse(0)

    println(s"<<< CONFIG >>> : input_dir=$input_dir, output_dir=$output_dir, update=$update")

    val buy_df: DataFrame = spark.read.json(s"$input_dir/buy/*")
    val view_df: DataFrame = spark.read.json(s"$input_dir/view/*")
    val input_df: DataFrame = buy_df.union(view_df)
    val input_date: String = input_df.select(max(col("date"))).head.get(0).toString
    println(s"<<< INPUT MAX DATE >>> : $input_date")

    val pivoted_df: DataFrame = input_df
      .withColumn("item_id", concat(col("event_type"), lit("_"), lower(regexp_replace(col("item_id"), "[ -]+", "_"))))
      .groupBy(col("uid"))
      .pivot(col("item_id"))
      .agg(count(col("item_id")))
      .na.fill(0)
    println("<<< PIVOTED DF >>>")

    val result_df = update match {
      case 0 => pivoted_df
      case 1 =>
        val hdfs = FileSystem.get(new java.net.URI(output_dir), spark.sparkContext.hadoopConfiguration)        
        val last_date = hdfs.listStatus(new Path(output_dir)).filter(_.isDirectory)
          .map(_.getPath.toString.split("/").last.toLong).max

        val old_df = spark.read.parquet(s"$output_dir/$last_date/*")

        val joined_df = old_df.join(pivoted_df, pivoted_df("uid") === old_df("uid"), "fullouter")
          .withColumn("uid-", coalesce(pivoted_df("uid"), old_df("uid")))
          .drop("uid")
          .withColumn("uid", col("uid-"))
          .drop("uid-")

        val shared_cols = pivoted_df.columns.intersect(old_df.columns).diff(Array("uid"))

        shared_cols.foldLeft(joined_df)((acc, column) => {
          acc.withColumn(s"$column-", coalesce(old_df(column), lit(0)) + coalesce(pivoted_df(column), lit(0)))
            .drop(column)
            .withColumn(column, col(s"$column-"))
            .drop(s"$column-")
        }).na.fill(0)
    }

    result_df
      .write
      .format("parquet")
      .mode("overwrite")
      .save(s"$output_dir/$input_date")

    println(s"<<< FINAL COLS NUM >>>: ${result_df.columns.length}")

  }
}
