import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object DF13 {

  def main(args:Array[String]):Unit= {

    val sc = new SparkConf()
    sc.set("spark.app.name", "SPark Df")
    sc.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sc)
      .getOrCreate()

    import spark.implicits._

//    3.Complex String Conditions: Data: A DataFrame documents with columns doc_id, content.
    val documents = List(
      (1, "The quick brown fox"),
      (2, "Lorem ipsum dolor sit amet"),
      (3, "Spark is a unified analytics engine")
    ).toDF("doc_id", "content")
//    Question: How would you add a new column content_category with values "Animal Related" if
//    content contains "fox", "Placeholder Text" if content contains "Lorem", and "Tech Related" if content
//    contains "Spark"?

    documents.select(col("doc_id"),col("content"),
      when(col("content").contains("fox"),"Animal Related")
        .when(col("content").contains("Lorem"),"Placeholder Text")
        .when(col("content").contains("Spark"),"Tech Related").alias("content_category")).show(false)


  }

}
