import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object DF8 {

  def main(args:Array[String]):Unit= {

    val sc = new SparkConf()
    sc.set("spark.app.name", "SPark Df")
    sc.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sc)
      .getOrCreate()

    import spark.implicits._
    val orders = List(
      (1, "2024-07-01"),
      (2, "2024-12-01"),
      (3, "2024-05-01")
    ).toDF("order_id", "order_date")
//    Question: How would you add a new column season with values "Summer" if order_date is in June,
//    July, or August, "Winter" if in December, January, or February, and "Other" otherwise?

    orders.select(col("order_id"),col("order_date"),
      when(col("order_date").contains("-06-") || col("order_date").contains("-07-") ||col("order_date").contains("-08-"),"Summer")
      .when(col("order_date").contains("-12-") || col("order_date").contains("-01-") ||col("order_date").contains("-02-"),"Winter")
      .otherwise("Other").alias("season")).show()


  }

}
