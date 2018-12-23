//    task7
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count,  org.apache.spark.sql.functions.max, org.apache.spark.sql.functions.abs
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._


object Task8 {
  def main(args: Array[String]): Unit = {


    SQLTask8()
  }

  def SQLTask8(): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameApi")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val pathUserWallPostsPath = "../Big data/data/bgdata_small/userWallPosts.parquet"

    val negative_emoji = "[😒😞😟😠😡😣☹🙁😕😔😖😫😩😤😮😦😯😰😨😱😧😭😢😵😥😲😪😓😷🤕🤒🙄💔😳]"


    spark.read.parquet(pathUserWallPostsPath).createOrReplaceGlobalTempView("user_wall_posts")

    val t0 = System.currentTimeMillis()
    spark.sql("SELECT COUNT(*) as count_negative FROM global_temp.user_wall_posts " +
      "WHERE text LIKE '%😒%' " +
      "OR text LIKE '%😞%' " +
      "OR text LIKE '%😟%' " +
      "OR text LIKE '%😠%' " +
      "OR text LIKE '%😡%' " +
      "OR text LIKE '%😣%' " +
      "OR text LIKE '%☹%' OR text LIKE '%😔%' OR text LIKE '%😤%' OR text LIKE '%😮%' OR text LIKE '%😯%' OR text LIKE '%😰%' " +
      "OR text LIKE '%😱%' " +
      "OR text LIKE '%😧%' " +
      "OR text LIKE '%😢%' " +
      "OR text LIKE '%😥%' " +
      "OR text LIKE '%😪%' " +
      "OR text LIKE '%😷%' " +
      "OR text LIKE '%🤕%' " +
      "OR text LIKE '%🤒%' " +
      "OR text LIKE '%🙄%' " +
      "OR text LIKE '%💔%' " +
      "OR text LIKE '%😳%' ")
      .createOrReplaceTempView("negative_count")


    val positive_emoji = "[]"
    spark.sql("SELECT COUNT(*) as count_positive FROM global_temp.user_wall_posts " +
      "WHERE text LIKE '%😀%' " +
      "OR text LIKE '%😊%' " +
      "OR text LIKE '%😄%' " +
      "OR text LIKE '%😌%' " +
      "OR text LIKE '%🤓%' " +
      "OR text LIKE '%😅%' " +
      "OR text LIKE '%😅%' OR text LIKE '%🙂%' OR text LIKE '%😍%' OR text LIKE '%😜%' OR text LIKE '%😎%' OR text LIKE '%😆%' " +
      "OR text LIKE '%🙃%' " +
      "OR text LIKE '%😘%' " +
      "OR text LIKE '%😝%' " +
      "OR text LIKE '%😂%' " +
      "OR text LIKE '%😇%' " +
      "OR text LIKE '%☺%' " +
      "OR text LIKE '%😛%' " +
      "OR text LIKE '%😃%' " +
      "OR text LIKE '%😋%' " +
      "OR text LIKE '%😙%' " +
      "OR text LIKE '%🤑%' " +
      "OR text LIKE '%💖%' " +
      "OR text LIKE '%💙%' " +
      "OR text LIKE '%♥%' " +
      "OR text LIKE '%🙈%' " +
      "OR text LIKE '%👧%' " +
      "OR text LIKE '%👩%' " +
      "OR text LIKE '%😈%' ")
      .createOrReplaceTempView("positive_count")


    val t1 = System.currentTimeMillis()

    println("DataFrameAPI. Time elapsed for task 8: ", t1-t0)
  }

  def dataFrameTask8(): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameApi")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val pathUserWallPostsPath = "../Big data/data/bgdata_small/userWallPosts.parquet"

    val negative_emoji = "[😒😞😟😠😡😣☹🙁😕😔😖😫😩😤😮😦😯😰😨😱😧😭😢😵😥😲😪😓😷🤕🤒🙄💔😳]"
    val positive_emoji = "[😀😄😊😌😚🤓😅🙂😍😜😎😆🙃😘😝🤗😂😇☺️😗😛😏😃😉😋😙😁🤑💖💙♥🙈👧👩😈]"

    val userWallPosts = spark.read.parquet(pathUserWallPostsPath)

    val t0 = System.currentTimeMillis()
    val negative_count = userWallPosts.filter(userWallPosts("text") =!= "").select("text").filter($"text" rlike negative_emoji).count()
    val positive_count = userWallPosts.filter(userWallPosts("text") =!= "").select("text").filter($"text" rlike positive_emoji).count()
    val t1 = System.currentTimeMillis()

    println("DataFrameAPI. Time elapsed for task 8: ", t1-t0)
    println(negative_count)
    println(positive_count)
    //    println("DataFrameAPI. Time elapsed for task 7: ", t1-t0)
  }



}
