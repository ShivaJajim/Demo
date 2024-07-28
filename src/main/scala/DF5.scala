import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object DF5 {

  def main(args:Array[String]):Unit= {

    val sc = new SparkConf()
    sc.set("spark.app.name", "SPark Df")
    sc.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sc)
      .getOrCreate()

    import spark.implicits._
    val events = List(
      (1, "2024-07-27"),
      (2, "2024-12-25"),
      (3, "2025-01-01")
    ).toDF("event_id", "date")

//    Question: How would you add a new column is_holiday which is true if the date is "2024-12-25" or
//      "2025-01-01", and false otherwise?

    events.select(col("event_id"),col("date"),when(col("date") === "2024-12-25"||col("date") === "2025-01-01","true")
      .otherwise("false").alias("is_holiday")).show()


  }

}
