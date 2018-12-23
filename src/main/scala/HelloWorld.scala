import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("HelloWorld")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val t0 = System.nanoTime()
    sc.textFile(path = "build.sbt")
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((one, anotherOne) => one + anotherOne)
      .foreach(println)
    val t1 = System.nanoTime()
//    System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) + "ns")
    Thread.sleep(100000)
  }
}