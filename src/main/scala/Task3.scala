//    task3
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count,  org.apache.spark.sql.functions.max, org.apache.spark.sql.functions.mean

object Task3 {
  def main(args: Array[String]): Unit = {
    sql_task3()
  }

  def sql_task3() : Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameApi")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val pathComments = "/Big data/data/bgdata_small/userWallComments.parquet"

    spark.read.parquet(pathComments).createOrReplaceGlobalTempView("comments")

    val t0 = System.currentTimeMillis()
    spark.sql("SELECT * FROM global_temp.comments WHERE from_id <> post_owner_id").createOrReplaceTempView("incoming_comments")
    spark.sql("SELECT count(id) as incoming_posts_comments_count, post_owner_id, post_id FROM incoming_comments " +
      "GROUP BY post_owner_id, post_id")
//        .show()
      .createOrReplaceTempView("incoming_comments_count")

    spark.sql("SELECT MAX(incoming_posts_comments_count), post_owner_id, post_id FROM incoming_comments_count " +
      "GROUP BY post_owner_id, post_id").createOrReplaceTempView("incoming_comments_count_max")

    spark.sql("SELECT MEAN(incoming_posts_comments_count), post_owner_id, post_id FROM incoming_comments_count " +
      "GROUP BY post_owner_id, post_id")
//        .show()
      .createOrReplaceTempView("incoming_comments_count_mean")

    val t1 = System.currentTimeMillis()
    println("DataFrameAPI. Time elapsed for task 3: ", t1-t0)

  }

  def dataFrameTask3(): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameApi")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val pathComments = "/Big data/data/bgdata_small/userWallComments.parquet"

    //    not equal =!=
    //    "made by other users"
    val comments = spark.read.parquet(pathComments)
    val t0 = System.currentTimeMillis()
    val incoming_posts = comments.filter(comments("from_id") =!= comments("post_owner_id"))

    val incoming_posts_count = incoming_posts.groupBy("post_owner_id", "post_id")
      .agg(count("id").alias("incoming_posts_comments_count"))
    //      .withColumnRenamed("count", "incoming_comments")

    //    incoming_posts_count.show(10)
    val incoming_max_mean = incoming_posts_count.groupBy("post_owner_id")
      .agg(max("incoming_posts_comments_count").alias("incoming_comments_max"),
        mean("incoming_posts_comments_count").alias("incoming_comments_mean"))

    //    incoming_max_mean.show(10)

    val cols = Seq("from_id", "incoming_posts_count", "incoming_comments_max", "incoming_comments_mean")
    val merged_count_max_mean = incoming_posts_count.join(incoming_max_mean, "post_owner_id").drop("post_id").toDF(cols:_*)
    //    merged_count_max_mean.show(10)

    //    val count_max_mean_data = incoming_max_mean.

    val t1 = System.currentTimeMillis()

    println("DataFrameAPI. Time elapsed for task 3: ", t1-t0)

  }

}
