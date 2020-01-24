import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.SparkSession

object SparkDatasetSample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    val seq = Seq(Person("hoge", 20), Person("foo", 30), Person("bar", 40))
    val ds = seq.toDS()
    ds.show()

    val ds2 = ds.withColumn("name", lit("hoge"))
    ds2.show()

    spark.stop()
  }
}
