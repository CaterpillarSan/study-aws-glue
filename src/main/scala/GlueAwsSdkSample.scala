import com.amazonaws.regions.Regions
import com.amazonaws.services.glue.model._
import com.amazonaws.services.glue.{AWSGlueClientBuilder, DynamicFrame, GlueContext}
import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object GlueAwsSdkSample {
  def main(sysArgs: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val glueContext = new GlueContext(spark.sparkContext)
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    // create sample data
    import spark.implicits._
    val seq = Seq(Person("hoge", 20), Person("foo", 30), Person("bar", 40))
    val df = seq.toDF()
    val dynamicFrame = DynamicFrame(df, glueContext)

    // write sample data to S3
    glueContext
      .getSinkWithFormat("s3", JsonOptions("path" -> "s3://yamashita.katsumi-glue/glue-awssdk-sample/"), format = "parquet")
      .writeDynamicFrame(dynamicFrame)

    // create table on data catalog
    val dbName = "awssdk_sample_db"
    val tableName = "awssdk_sample"
    val client = AWSGlueClientBuilder.standard().withRegion(Regions.AP_NORTHEAST_1).build()
    try {
      client.createDatabase(new CreateDatabaseRequest().withDatabaseInput(new DatabaseInput().withName(dbName)))
    } catch {
      case e: AlreadyExistsException => println(e)
    }
    try {
      client.createTable(
        new CreateTableRequest().withDatabaseName(dbName).withTableInput(
          new TableInput().withName(tableName).withStorageDescriptor(
            new StorageDescriptor().withColumns(
              new Column().withName("name").withType("string"),
              new Column().withName("age").withType("int")
            ).withLocation("s3://yamashita.katsumi-glue/glue-awssdk-sample-db/")
              .withInputFormat("org.apache.hadoop.mapred.TextInputFormat")
              .withOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
              .withSerdeInfo(new SerDeInfo().withSerializationLibrary("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))
          )
        )
      )
    } catch {
      case e: AlreadyExistsException => println(e)
    }

    // write sample data to table
    glueContext.getCatalogSink(dbName, tableName).writeDynamicFrame(dynamicFrame)

    spark.stop()
    Job.commit()
  }
}
