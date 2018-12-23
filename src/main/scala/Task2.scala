//    task2
import org.apache.spark.sql.SparkSession

object Task2 {
  def main(args: Array[String]): Unit = {
    sql_task2()
  }

  def sql_task2() : Unit ={
    val spark = SparkSession.builder()
      .appName("DataFrameApi")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val pathFriends = "../Big data/data/bgdata_small/friends.parquet"
    val pathFollowers = "../Big data/data/bgdata_small/followers.parquet"
    val pathUsergroups = "../Big data/data/bgdata_small/userGroupsSubs.parquet"
    val pathFollowerProfiles = "../Big data/data/bgdata_small/followerProfiles.parquet"
    val pathFriendsrProfiles = "../Big data/data/bgdata_small/friendsProfiles.parquet"

    spark.read.parquet(pathFriends).createOrReplaceGlobalTempView("friends")
    spark.read.parquet(pathFollowers).createOrReplaceGlobalTempView("followers")
    spark.read.parquet(pathUsergroups).createOrReplaceGlobalTempView("userGroups")

    val t0 = System.currentTimeMillis()
    spark.sql("SELECT f.profile as from_id, count(f.profile) as friends_count " +
      "FROM global_temp.friends f " +
      "GROUP BY f.profile").createOrReplaceTempView("friends_count_table")

    spark.sql("SELECT fo.profile as from_id, count(fo.profile) as followers_count " +
      "FROM global_temp.followers fo " +
      "GROUP BY fo.profile").createOrReplaceTempView("followers_count_table")

    spark.sql("SELECT u.user as from_id, count(u.user) as groups_count " +
      "FROM global_temp.userGroups u " +
      "GROUP BY u.user").createOrReplaceTempView("groups_count_table")

    spark.read.parquet(pathFollowerProfiles).createOrReplaceGlobalTempView("followerProfiles")
    spark.sql("SELECT * from global_temp.followerProfiles WHERE counters IS NOT NULL").createOrReplaceTempView("followers_counters_table")

    spark.sql("SELECT id as from_id, counters.videos, counters.audios, counters.photos, counters.gifts from followers_counters_table")
      .createOrReplaceTempView("additional_counters_table")

    spark.sql("SELECT f.from_id, f.friends_count, " +
      "fo.followers_count, g.groups_count, " +
      "ad.videos, ad.audios, ad.photos, ad.gifts " +
      "FROM friends_count_table f " +
      "JOIN followers_count_table fo " +
      "ON f.from_id = fo.from_id " +
      "JOIN groups_count_table g " +
      "ON f.from_id = g.from_id " +
      "JOIN additional_counters_table as ad " +
      "ON f.from_id = ad.from_id").show()

    val t1 = System.currentTimeMillis()

    println("DataFrameAPI. Time elapsed for task 2: ", t1-t0)

  }

  def dataFrame_task2(): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameApi")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val pathFriends = "../Big data/data/bgdata_small/friends.parquet"
    val pathFollowers = "../Big data/data/bgdata_small/followers.parquet"
    val pathUsergroups = "../Big data/data/bgdata_small/userGroupsSubs.parquet"
    val pathFollowerProfiles = "../Big data/data/bgdata_small/followerProfiles.parquet"

    val friends = spark.read.parquet(pathFriends)
    val followers = spark.read.parquet(pathFollowers)
    val userGroups = spark.read.parquet(pathUsergroups)

    val t0 = System.currentTimeMillis()
    val friends_count = friends.groupBy("profile").count()
    val friends_names = Seq("from_id", "friends_count")
    val friends_data = friends_count.toDF(friends_names: _*)

    val followers_count = followers.groupBy("profile").count()
    val followers_names = Seq("from_id", "followers_count")
    val followers_data = followers_count.toDF(followers_names: _*)

    val groups_count = userGroups.groupBy("user").count()
    val groups_names = Seq("from_id", "groups_count")
    val groups_data = groups_count.toDF(groups_names: _*)

    followers_data.join(friends_data, Seq("from_id")).join(groups_data, Seq("from_id"))

    val friends_profiles = spark.read.parquet(pathFollowerProfiles)

    //    friends_profiles.show()

    val friends_profiles_table = friends_profiles.filter(friends_profiles("counters").isNotNull)

    //      : _* is a special instance of type ascription which tells the compiler to treat a single argument of a sequence type as a variable argument sequence, i.e. varargs.
    val cols = Seq("id","counters")
    val data1 = friends_profiles_table.select(cols.head, cols.tail: _*)
    val data2 = friends_profiles_table.select(cols.head, cols.tail: _*).select(data1("id"), data1("counters.videos"), data1("counters.audios"),
      data1("counters.photos"),data1("counters.gifts"))

    val stat = Seq("from_id", "videos", "audios", "photos", "gifts")
    val stat_fin = data2.toDF(stat: _*)

    val t1 = System.currentTimeMillis()

    println("DataFrameAPI. Time elapsed for task 2: ", t1-t0)
    stat_fin.show(10)

    //    friends_profiles_table.show()
  }
}

