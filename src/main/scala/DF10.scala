import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object DF10 {

  def main(args:Array[String]):Unit= {

    val sc = new SparkConf()
    sc.set("spark.app.name", "SPark Df")
    sc.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sc)
      .getOrCreate()

    import spark.implicits._
    val logins = List(
      (1, "09:00"),
      (2, "18:30"),
      (3, "14:00")
    ).toDF("login_id", "login_time")
//    Question: How would you add a new column is_morning which is true if login_time is before 12:00,
//    and false otherwise?

    logins.select(col("login_id"),col("login_time"),
      when(col("login_time")< "12:00","true").otherwise("false")
        .alias("is_morning")).show()


  }

}
