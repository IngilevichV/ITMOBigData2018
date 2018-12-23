//    task3
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count,  org.apache.spark.sql.functions.max, org.apache.spark.sql.functions.abs
import org.apache.spark.sql.types.IntegerType

object Task6 {
  def main(args: Array[String]): Unit = {
    sqkTask6()
  }

  def sqkTask6(): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameApi")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val pathFriendsProfiles = "../Big data/data/bgdata_small/friendsProfiles.parquet"
    val pathFriends = "../Big data/data/bgdata_small/friends.parquet"

    spark.read.parquet(pathFriendsProfiles).createOrReplaceGlobalTempView("friends_profiles")
    spark.read.parquet(pathFriends).createOrReplaceGlobalTempView("friends")

    val t0 = System.currentTimeMillis()

    spark.sql("SELECT * FROM global_temp.friends_profiles " +
      "WHERE deactivated IS NULL")
      .createOrReplaceTempView("friends_profiles_filter")

    spark.sql("SELECT * " +
      "FROM global_temp.friends f " +
      "JOIN friends_profiles_filter fpf " +
      "ON f.profile = fpf.id ").createOrReplaceTempView("merged_friends")

    spark.sql("SELECT COUNT(*)" +
      "FROM merged_friends " +
      "GROUP BY profile").createOrReplaceTempView("merged_friends_count")

    val t1 = System.currentTimeMillis()

    println("SQLAPI. Time elapsed for task 6: ", t1-t0)

  }

  def dataFrameTask6(): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameApi")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val pathFriendsProfiles = "../Big data/data/bgdata_small/friendsProfiles.parquet"
    val pathFriends = "../Big data/data/bgdata_small/friends.parquet"

    val friendsProfiles = spark.read.parquet(pathFriendsProfiles)
    val friends = spark.read.parquet(pathFriends)

    val t0 = System.currentTimeMillis()
    //    count of deleted users in friends
    var deact_filt = friendsProfiles.filter(friendsProfiles("deactivated").isNull)
    var deact_friends = friends.join(deact_filt, deact_filt("id") === friends("profile")).groupBy("profile").count()

    val t1 = System.currentTimeMillis()

    println("DataFrameAPI. Time elapsed for task 6: ", t1-t0)

  }

}
