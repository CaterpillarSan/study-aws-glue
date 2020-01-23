import com.amazonaws.services.glue.DynamicFrame
import com.amazonaws.services.glue.GlueContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf

object GlueSample {
  /**
   * サンプルの書き換え処理
   * nameが"hoge"だったらnameに"piyo"をつけてageを100足す
   */
  val customFilter: (DataFrame) => DataFrame = (df: DataFrame) => {
    // FIXME: foldLeftで書く?
    // https://stackoverflow.com/questions/41400504/spark-scala-repeated-calls-to-withcolumn-using-the-same-function-on-multiple-c
    // https://medium.com/@mrpowers/performing-operations-on-multiple-columns-in-a-spark-dataframe-with-foldleft-42133f82de04
    val filterName = udf {
      (name: String) => if (name == "hoge") name + "piyo" else name
    }
    val filterAge = udf {
      (name: String, age: Int) => if (name == "hoge") age + 100 else age
    }
    df.withColumn("age", filterAge(col("name"), col("age")))
      .withColumn("name", filterName(col("name")))  // nameを書き換えてしまうので他の処理より後にしないといけない
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate
    val gc = new GlueContext(spark.sparkContext)

    import spark.implicits._
    val seq = Seq(Person("hoge", 20), Person("foo", 30), Person("bar", 40))
    val df = seq.toDF()
    val dynamicFrame = DynamicFrame(df, gc)
    dynamicFrame.show()

    val dataFrame = dynamicFrame.toDF()
    val dataFrame2 = customFilter(dataFrame)

    val dynamicFrame2 = DynamicFrame(dataFrame2, gc)
    dynamicFrame2.show()

    spark.stop()
  }
}
