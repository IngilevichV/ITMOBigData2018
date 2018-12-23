//    task3
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count,  org.apache.spark.sql.functions.max, org.apache.spark.sql.functions.abs
import org.apache.spark.sql.types.IntegerType

object Task5 {
  def main(args: Array[String]): Unit = {
    sqlTask5()
  }

  def sqlTask5(): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameApi")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val pathGroupsProfiles = "../Big data/data/bgdata_small/groupsProfiles.parquet"
    val pathUserGroups = "../Big data/data/bgdata_small/userGroupsSubs.parquet"

    spark.read.parquet(pathGroupsProfiles).createOrReplaceGlobalTempView("groups_profiles")
    spark.read.parquet(pathUserGroups).createOrReplaceGlobalTempView("user_groups")

    val t0 = System.currentTimeMillis()
    spark.sql("SELECT user, group*-1 as group_invert FROM global_temp.user_groups " +
      "GROUP BY user, key, group")
      .createOrReplaceTempView("user_groups_table")


    spark.sql("SELECT id, is_closed as group_invert FROM  global_temp.groups_profiles " +
      "GROUP BY id, is_closed").createOrReplaceTempView("groups_profiles_inverted")

    spark.sql("SELECT count(*) as count_opened, user as from_id FROM  user_groups_table WHERE is_closed = 0 " +
      "GROUP BY user ")
//      .createOrReplaceTempView("count_opened_groups")
      .coalesce(1).write.format("com.databricks.spark.csv")
      .save("task5a/table5a.csv")

    spark.sql("SELECT count(*) as count_closed, user as from_id FROM  user_groups_table WHERE is_closed > 0 " +
      "GROUP BY user ")
      .coalesce(1).write.format("com.databricks.spark.csv")
      .save("task5b/table5b.csv")
//      .createOrReplaceTempView("count_closed_groups")

    val t1 = System.currentTimeMillis()

    println("DataFrameAPI. Time elapsed for task 5: ", t1-t0)


    //      .createOrReplaceTempView("incoming_likes")
  }

  def dataFrameTask5(): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameApi")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val pathGroupsProfiles = "../Big data/data/bgdata_small/groupsProfiles.parquet"
    val pathUserGroups = "../Big data/data/bgdata_small/userGroupsSubs.parquet"

    val groups = spark.read.parquet(pathGroupsProfiles)
    val user_groups = spark.read.parquet(pathUserGroups)

    val t0 = System.currentTimeMillis()

//    Меняем тип для правильной обработки
    user_groups.withColumn("group_tmp", user_groups("group").cast(IntegerType))
      .drop("group")
      .withColumnRenamed("group_tmp", "id")

    val user_groups2 = user_groups.withColumn("group_tmp", user_groups("group").cast(IntegerType))
      .drop("group")
      .withColumnRenamed("group_tmp", "id")

    val user_groups3 = user_groups2.withColumn("user_tmp", user_groups("user").cast(IntegerType))
      .drop("user")
      .withColumnRenamed("user_tmp", "user")
    val user_groups4 = user_groups3.withColumn("id", abs(user_groups3("id")))

    val merged = user_groups4.join(groups, Seq("id"))

    val groups_open = merged.filter(merged("is_closed") === 0)
    val groups_open_cnt = groups_open.groupBy("user").count()
    val groups_closed = merged.filter(merged("is_closed") === 1)
    val groups_closed_cnt = groups_closed.groupBy("user").count()

    val groups_fin1 = groups_open_cnt.join(groups_closed_cnt, Seq("user"))
    val groups_fin2 = Seq("from_id", "open_groups_count", "closed_groups_count")
    val groups_fin = groups_fin1.toDF(groups_fin2: _*)


    val t1 = System.currentTimeMillis()

    println("DataFrameAPI. Time elapsed for task 5: ", t1-t0)

  }


}
