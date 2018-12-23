//    task7
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count,  org.apache.spark.sql.functions.max, org.apache.spark.sql.functions.abs
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._

object Task7 {
  def main(args: Array[String]): Unit = {
    SQLTask7()
  }

  def SQLTask7(): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameApi")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val pathFriends = "../Big data/data/bgdata_small/followers.parquet"
    val pathComments = "../Big data/data/bgdata_small/userWallComments.parquet"
    val pathLikes = "../Big data/data/bgdata_small/userWallLikes.parquet"

    spark.read.parquet(pathComments).createOrReplaceGlobalTempView("Comments")
    spark.read.parquet(pathLikes).createOrReplaceGlobalTempView("Likes")
    spark.read.parquet(pathFriends).createOrReplaceGlobalTempView("Friends")

    val t0 = System.currentTimeMillis()

    spark.sql("SELECT id, post_owner_id, from_id, count(from_id) as count_from_id FROM global_temp.Comments " +
      "WHERE from_id > 0 GROUP BY id, post_owner_id, from_id")
//        .show()
      .createOrReplaceTempView("Comments_count")

    spark.sql("SELECT profile as from_id, SUM(cc.count_from_id), MAX(cc.count_from_id), MEAN(cc.count_from_id) " +
      "FROM Comments_count cc " +
      "JOIN global_temp.Friends f " +
      "ON (cc.from_id = CAST(f.follower as INT) " +
      "AND cc.post_owner_id = CAST(f.profile as INT))" +
      "GROUP BY profile")
//        .show()
//      .createOrReplaceTempView("Comments_count_table")


    spark.sql("SELECT itemId, ownerId, likerId, count(likerId) as count_likerId FROM global_temp.Likes " +
      "WHERE likerId > 0 GROUP BY itemId, ownerId, likerId")
      .createOrReplaceTempView("Likes_count")

    spark.sql("SELECT profile as from_id, SUM(lc.count_likerId), MAX(lc.count_likerId), MEAN(lc.count_likerId) " +
      "FROM Likes_count lc " +
      "JOIN global_temp.Friends f " +
      "ON (lc.likerId = CAST(f.follower as INT) " +
      "AND lc.ownerId = CAST(f.profile as INT))" +
      "GROUP BY profile")
      .show()


    val t1 = System.currentTimeMillis()

    println("DataFrameAPI. Time elapsed for task 7: ", t1-t0)

  }

  def DataFrameTask7(): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameApi")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val pathFriends = "../Big data/data/bgdata_small/followers.parquet"
    val pathComments = "../Big data/data/bgdata_small/userWallComments.parquet"
    val pathLikes = "../Big data/data/bgdata_small/userWallLikes.parquet"

    val comments = spark.read.parquet(pathComments)
    val likes = spark.read.parquet(pathLikes)
    val friends = spark.read.parquet(pathFriends)

    val t0 = System.currentTimeMillis()

    val comments_count = comments
      .filter("from_id > 0")
      .groupBy(col("id"), col("post_owner_id"), col("from_id"))
      .count()

    val comments_characteristic = friends
      .join(comments_count,
        comments_count("from_id") === friends("follower").cast("int") &&
          comments_count("post_owner_id") === friends("profile").cast("int"))
      .groupBy("profile")
      .agg(
        sum("count").alias("comments_count"),
        max("count").alias("comments_max"),
        mean("count").alias("comments_mean")
      )
      .withColumnRenamed("profile", "from_id")

    val likes_count = likes
      .filter("likerId > 0")
      .groupBy(col("itemId"), col("ownerId"), col("likerId"))
      .count()

    val likes_characteristic = friends
      .join(likes_count,
        likes_count("likerId") === friends("follower").cast("int") &&
          likes_count("ownerId") === friends("profile").cast("int"))
      .groupBy("profile")
      .agg(
        sum("count").alias("friends_likes_count"),
        max("count").alias("friends_likes_max"),
        mean("count").alias("friends_likes_mean")
      )
      .withColumnRenamed("profile", "from_id")

    val t1 = System.currentTimeMillis()

    println("DataFrameAPI. Time elapsed for task 7: ", t1-t0)

  }
}
