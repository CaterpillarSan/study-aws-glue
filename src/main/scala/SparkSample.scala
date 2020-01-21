import org.apache.spark.SparkContext

object SparkSample {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "example")
    val data = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val rdd = sc.parallelize(data)
    val sum = rdd.reduce(_ + _)
    println(s"sum=$sum")
    sc.stop()
  }
}
