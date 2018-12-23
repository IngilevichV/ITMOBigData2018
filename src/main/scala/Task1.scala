//    task1
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, SQLContext}


object Task1 {
  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
    sql_task1()
//    spark_native_task1()

  }

  def spark_native_task1(): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameApi")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val pathPosts = "../Big data/data/bgdata_small/userWallPosts.parquet"

    spark.read.parquet(pathPosts).map(attributes => "Name: " + attributes(0)).show()

  }

  def sql_task1(): Unit = {
    val spark = SparkSession.builder()
      .appName("SQLTask1")
      .master("local")
      .getOrCreate()


    val pathPosts = "../Big data/data/bgdata_small/userWallPosts.parquet"
    val pathComments = "../Big data/data/bgdata_small/userWallComments.parquet"
    val pathLikes = "../Big data/data/bgdata_small/userWalllikes.parquet"

    spark.read.parquet(pathPosts).createOrReplaceGlobalTempView("posts")
    spark.read.parquet(pathComments).createOrReplaceGlobalTempView("comments")
    spark.read.parquet(pathLikes).createOrReplaceGlobalTempView("likes")

    val t0 = System.currentTimeMillis()
    spark.sql("SELECT from_id, count(from_id) as count_posts FROM global_temp.posts group by from_id").createOrReplaceTempView("posts_count")
    spark.sql("SELECT from_id, count(from_id) as count_comments FROM global_temp.comments group by from_id").createOrReplaceTempView("comments_count")
    spark.sql("SELECT likerId as from_id, count(likerId) as count_likes FROM global_temp.likes group by from_id").createOrReplaceTempView("likes_count")


    spark.sql("SELECT p.from_id, p.count_posts, c.count_comments, l.count_likes " +
      "FROM posts_count p " +
      "JOIN comments_count c " +
      "ON p.from_id = c.from_id " +
      "JOIN likes_count l " +
      "ON p.from_id = l.from_id").show()

//    likes_count.show()
    val t1 = System.currentTimeMillis()

    println("spark SQL API time elapsed: ", t1 - t0)
  }

  def dataFrame_task1(): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameApi")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val pathPosts = "../Big data/data/bgdata_small/userWallPosts.parquet"
    val pathComments = "../Big data/data/bgdata_small/userWallComments.parquet"
    val pathLikes = "../Big data/data/bgdata_small/userWalllikes.parquet"

    val posts = spark.read.parquet(pathPosts)
    val comments = spark.read.parquet(pathComments)
    val likes = spark.read.parquet(pathLikes)

    val t0 = System.currentTimeMillis()
    val posts_count = posts.select("from_id")
      .groupBy("from_id")
      .count()
      .withColumnRenamed("from_id", "user_id")
      .withColumnRenamed("count", "posts_count")

    val likes_count = likes.select("likerId")
      .groupBy("likerId")
      .count()
      .withColumnRenamed("likerId", "user_id")
      .withColumnRenamed("count", "likes_count")

    val comments_count = comments.select("from_id")
      .groupBy("from_id")
      .count()
      .withColumnRenamed("from_id", "user_id")
      .withColumnRenamed("count", "comments_count")

    val merged_table = comments_count
      .join(posts_count, "user_id")
      .join(comments_count, "user_id")
      .join(likes_count, "user_id")

    val t1 = System.currentTimeMillis()
    println("DataFrame API time elapsed: ", t1 - t0)
    merged_table.show()
    sql_task1()
  }


}