//    task3
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count,  org.apache.spark.sql.functions.max, org.apache.spark.sql.functions.mean

object Task4 {
  def main(args: Array[String]): Unit = {
//    DataFrameTask4()
    SQLTask4()
  }

  def SQLTask4(): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameApi")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val pathLikes = "../data/bgdata_small/userWallLikes.parquet"

    spark.read.parquet(pathLikes).createOrReplaceGlobalTempView("likes")

    val t0 = System.currentTimeMillis()
    spark.sql("SELECT * FROM global_temp.likes WHERE likerId <> ownerId").createOrReplaceTempView("incoming_likes")
    spark.sql("SELECT count(key) as incoming_likes_count, ownerId, itemId FROM incoming_likes " +
      "GROUP BY ownerId, itemId")
      //        .show()
      .createOrReplaceTempView("incoming_likes_count_table")

    val results = spark.sql("SELECT MAX(incoming_likes_count) as max_incoming_likes_count, MEAN(incoming_likes_count) as mean_incoming_likes_count, ownerId as from_id, itemId FROM incoming_likes_count_table " +
      "GROUP BY ownerId, itemId")

    results.write.format("parquet").save("results.parquet")
//      .write.parquet("table_test")


    val t1 = System.currentTimeMillis()
    println("DataFrameAPI. Time elapsed for task 4: ", t1-t0)
  }

  def DataFrameTask4(): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameApi")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val pathLikes = "../data/bgdata_small/userWallLikes.parquet"

    //    not equal =!=
    //    "made by other users"
    val likes = spark.read.parquet(pathLikes)
    val t0 = System.currentTimeMillis()
    val incoming_likes = likes.filter(likes("likerId") =!= likes("ownerId"))

    val incoming_likes_count = incoming_likes.groupBy("ownerId", "itemId")
      .agg(count("key").alias("incoming_likes"))
    //      .agg(count("id").alias("incoming_posts_comments_count"))
    //      .withColumnRenamed("count", "incoming_comments")

    //    incoming_posts_count.show(10)
    val incoming_max_mean = incoming_likes_count.groupBy("ownerId")
      .agg(max("incoming_likes").alias("incoming_likes_max"),
        mean("incoming_likes").alias("incoming_likes_mean"))

    //    incoming_max_mean.show(10)

    val cols = Seq("from_id", "incoming_likes_count", "incoming_likes_max", "incoming_likes_mean")
    val merged_count_max_mean = incoming_likes_count.join(incoming_max_mean, "ownerId").drop("itemId").toDF(cols:_*)

    //    val count_max_mean_data = incoming_max_mean.

    val t1 = System.currentTimeMillis()

    println("DataFrameAPI. Time elapsed for task 4: ", t1-t0)

  }
}
