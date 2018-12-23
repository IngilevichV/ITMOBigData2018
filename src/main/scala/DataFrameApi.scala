//    task1
import org.apache.spark.sql.SparkSession

object DataFrameApi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("DataFrameApi")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val pathPosts = "C:\\Users\\varva\\Desktop\\ITMO_17_19\\Big data\\data\\bgdata_small\\userWallPosts.parquet"
    val pathComments = "C:\\Users\\varva\\Desktop\\ITMO_17_19\\Big data\\data\\bgdata_small\\userWallComments.parquet"
    val pathLikes = "C:\\Users\\varva\\Desktop\\ITMO_17_19\\Big data\\data\\bgdata_small\\userWalllikes.parquet"

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
    println("DataFrame API time elapsed: ", (t1-t0))
    merged_table.show()

//    task2


    val pathFriends = "C:\\Users\\varva\\Desktop\\ITMO_17_19\\Big data\\data\\bgdata_small\\friends.parquet"
    val pathFollowers = "C:\\Users\\varva\\Desktop\\ITMO_17_19\\Big data\\data\\bgdata_small\\\followers.parquet"
    val pathUsergroups = "C:\\Users\\varva\\Desktop\\ITMO_17_19\\Big data\\data\\bgdata_small\\userGroupsSubs.parquet"
    val pathFollowerProfiles = "C:\\Users\\varva\\Desktop\\ITMO_17_19\\Big data\\data\\bgdata_small\\followerProfiles.parquet"

    val friends_profiles = spark.read.parquet(pathFollowerProfiles)

    friends_profiles.show()

//    val t0 = System.currentTimeMillis()
    val friends_profiles_table = friends_profiles.filter(friends_profiles("counters").isNotNull)

//      : _* is a special instance of type ascription which tells the compiler to treat a single argument of a sequence type as a variable argument sequence, i.e. varargs.
    val cols = Seq("id","counters")
    val data1 = friends_profiles_table.select(cols.head, cols.tail: _*)
    val data2 = friends_profiles_table.select(cols.head, cols.tail: _*).select(data1("id"), data1("counters.videos"), data1("counters.audios"),
      data1("counters.photos"),data1("counters.gifts"))

    val stat = Seq("from_id", "videos", "audios", "photos", "gifts")
    val stat_fin = data2.toDF(stat: _*)

//    val t1 = System.currentTimeMillis()

//    println("time elapsed for task 2: ", t1-t0)
    stat_fin.show(10)

//    friends_profiles_table.show()
  }
}
