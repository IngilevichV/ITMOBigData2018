//    task1
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, SQLContext}


object Task9 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_HOME", "C:/winutils")

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


    spark.sql("SELECT from_id, count(from_id) as count_posts FROM global_temp.posts group by from_id").createOrReplaceTempView("posts_count")
    spark.sql("SELECT from_id, count(from_id) as count_comments FROM global_temp.comments group by from_id").createOrReplaceTempView("comments_count")
    spark.sql("SELECT likerId as from_id, count(likerId) as count_likes FROM global_temp.likes group by from_id").createOrReplaceTempView("likes_count")

    spark.sql("SELECT p.from_id, p.count_posts, c.count_comments, l.count_likes " +
      "FROM posts_count p " +
      "JOIN comments_count c " +
      "ON p.from_id = c.from_id " +
      "JOIN likes_count l " +
      "ON p.from_id = l.from_id").createOrReplaceTempView("task1")

    val pathFriends = "../Big data/data/bgdata_small/friends.parquet"
    val pathFollowers = "../Big data/data/bgdata_small/followers.parquet"
    val pathUsergroups = "../Big data/data/bgdata_small/userGroupsSubs.parquet"
    val pathFollowerProfiles = "../Big data/data/bgdata_small/followerProfiles.parquet"
    val pathFriendsrProfiles = "../Big data/data/bgdata_small/friendsProfiles.parquet"


    spark.read.parquet(pathFriends).createOrReplaceGlobalTempView("friends")
    spark.read.parquet(pathFollowers).createOrReplaceGlobalTempView("followers")
    spark.read.parquet(pathUsergroups).createOrReplaceGlobalTempView("userGroups")

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
      "ON f.from_id = ad.from_id").createOrReplaceTempView("task2")



//    spark.read.parquet(pathComments).createOrReplaceGlobalTempView("comments")

    spark.sql("SELECT * FROM global_temp.comments WHERE from_id <> post_owner_id").createOrReplaceTempView("incoming_comments")
    spark.sql("SELECT count(id) as incoming_posts_comments_count, post_owner_id, post_id FROM incoming_comments " +
      "GROUP BY post_owner_id, post_id")
      //        .show()
      .createOrReplaceTempView("incoming_comments_count")

    spark.sql("SELECT MAX(incoming_posts_comments_count), post_owner_id, post_id FROM incoming_comments_count " +
      "GROUP BY post_owner_id, post_id").createOrReplaceTempView("incoming_comments_count_max")

    spark.sql("SELECT MEAN(incoming_posts_comments_count) as mean_incoming_posts_comments_count, post_owner_id as from_id, post_id FROM incoming_comments_count " +
      "GROUP BY post_owner_id, post_id")
      //        .show()
      .createOrReplaceTempView("task3")



    spark.sql("SELECT * FROM global_temp.likes WHERE likerId <> ownerId").createOrReplaceTempView("incoming_likes")
    spark.sql("SELECT count(key) as incoming_likes_count, ownerId, itemId FROM incoming_likes " +
      "GROUP BY ownerId, itemId")
      //        .show()
      .createOrReplaceTempView("incoming_likes_count_table")

    spark.sql("SELECT MAX(incoming_likes_count), MEAN(incoming_likes_count), ownerId as from_id, itemId FROM incoming_likes_count_table " +
      "GROUP BY ownerId")
      .createOrReplaceTempView("task4")




    val pathFriendsProfiles = "../Big data/data/bgdata_small/friendsProfiles.parquet"
//    val pathFriends = "../Big data/data/bgdata_small/friends.parquet"

    spark.read.parquet(pathFriendsProfiles).createOrReplaceGlobalTempView("friends_profiles")
    spark.read.parquet(pathFriends).createOrReplaceGlobalTempView("friends")


    spark.sql("SELECT * FROM global_temp.friends_profiles " +
      "WHERE deactivated IS NULL")
      .createOrReplaceTempView("friends_profiles_filter")

    spark.sql("SELECT * " +
      "FROM global_temp.friends f " +
      "JOIN friends_profiles_filter fpf " +
      "ON f.profile = fpf.id ").createOrReplaceTempView("merged_friends")

    spark.sql("SELECT COUNT(*) as friends_count, profile as from_id " +
      "FROM merged_friends " +
      "GROUP BY profile").createOrReplaceTempView("task6")

    spark.read.parquet(pathComments).createOrReplaceGlobalTempView("Comments")
    spark.read.parquet(pathLikes).createOrReplaceGlobalTempView("Likes")
    spark.read.parquet(pathFriends).createOrReplaceGlobalTempView("Friends")



    spark.sql("SELECT id, post_owner_id, from_id, count(from_id) as count_from_id FROM global_temp.Comments " +
      "WHERE from_id > 0 GROUP BY id, post_owner_id, from_id")
      //        .show()
      .createOrReplaceTempView("Comments_count")

    spark.sql("SELECT profile as from_id, SUM(cc.count_from_id) as sum_comments, MAX(cc.count_from_id) as max_comments, MEAN(cc.count_from_id) as " +
      "mean_comments " +
      "FROM Comments_count cc " +
      "JOIN global_temp.Friends f " +
      "ON (cc.from_id = CAST(f.follower as INT) " +
      "AND cc.post_owner_id = CAST(f.profile as INT))" +
      "GROUP BY profile")
    //        .show()
          .createOrReplaceTempView("task7_a")


    spark.sql("SELECT itemId, ownerId, likerId, count(likerId) as count_likerId FROM global_temp.Likes " +
      "WHERE likerId > 0 GROUP BY itemId, ownerId, likerId")
      .createOrReplaceTempView("Likes_count")

    spark.sql("SELECT profile as from_id, SUM(lc.count_likerId) as sum_likes, MAX(lc.count_likerId) as max_likes, " +
      "MEAN(lc.count_likerId) as mean_likes " +
      "FROM Likes_count lc " +
      "JOIN global_temp.Friends f " +
      "ON (lc.likerId = CAST(f.follower as INT) " +
      "AND lc.ownerId = CAST(f.profile as INT))" +
      "GROUP BY profile")
      .createOrReplaceTempView("task7_b")


    val t0 = System.currentTimeMillis()
    val overall = spark.sql("SELECT t1.from_id, t1.count_posts, t1.count_comments, t1.count_likes, t2.friends_count, t2.followers_count, t2.groups_count, " +
      "t2.videos, t2.audios, t2.photos, t2.gifts, " +
      "t3.mean_incoming_posts_comments_count, t6.friends_count as friends_count_v2, " +
      "t7a.sum_comments, t7a.max_comments, t7a.mean_comments, " +
      "t7b.sum_likes, t7b.max_likes, t7b.mean_likes " +
      "FROM task1 t1 " +
      "JOIN task2 t2 " +
      "ON t1.from_id = t2.from_id " +
      "JOIN task3 t3 " +
      "ON t1.from_id = t3.from_id " +
      "JOIN task4 t4 " +
      "ON t1.from_id = t4.from_id " +
      "JOIN task5_a t5a " +
      "ON t1.from_ID = t5a.from_id" +
      "JOIN task5b t5b " +
      "ON t1.from_id = t5b.from_id " +
      "JOIN task6 t6 " +
      "ON t1.from_id = t6.from_id " +
      "JOIN task7_a t7a " +
      "ON t1.from_id = t7a.from_id " +
      "JOIN task7_b t7b " +
      "ON t1.from_id = t7b.from_id")

    overall.write.format("parquet").save("results_new.parquet")

    val t1 = System.currentTimeMillis()

    println("DataFrameAPI. Time elapsed for task 7: ", t1-t0)

  }
}
