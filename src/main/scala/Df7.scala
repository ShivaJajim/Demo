import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object DF7 {

  def main(args:Array[String]):Unit= {

    val sc = new SparkConf()
    sc.set("spark.app.name", "SPark Df")
    sc.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sc)
      .getOrCreate()

    import spark.implicits._
    val customers = List(
      (1, "john@gmail.com"),
      (2, "jane@yahoo.com"),
      (3, "doe@hotmail.com")
    ).toDF("customer_id", "email")
//    Question: How would you add a new column email_provider with values "Gmail" if email contains
//      "gmail", "Yahoo" if email contains "yahoo", and "Other" otherwise?

    customers.select(col("customer_id"),col("email"),when(col("email").contains("gmail") ,"gmail")
      .when(col("email").contains("yahoo") ,"Yahoo")
      .otherwise("Other").alias("email_provider")).show()


  }

}
