import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object DF14 {

  def main(args:Array[String]):Unit= {

    val sc = new SparkConf()
    sc.set("spark.app.name", "SPark Df")
    sc.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sc)
      .getOrCreate()

    import spark.implicits._

   // 4.Multiple Date Conditions: Data: A DataFrame tasks with columns task_id, start_date, end_date.
    val tasks = List(
      (1, "2024-07-01", "2024-07-10"),
      (2, "2024-08-01", "2024-08-15"),
      (3, "2024-09-01", "2024-09-05")
    ).toDF("task_id", "start_date", "end_date")
//    Question: How would you add a new column task_duration which is "Short" if the difference
//      between end_date and start_date is less than 7 days, "Medium" if it is between 7 and 14 days, and
//    "Long" otherwise?

    tasks.select(col("task_id"),col("start_date"),col("end_date"),
      when(col("end_date")-col("start_date") <7,"Short")
        .when(col("end_date")-col("start_date") >7 && col("end_date")-col("start_date") <14,"Short")
        .otherwise("Long")

        .alias("task_duration")).show(false)


  }

}
