import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object DF9 {

  def main(args:Array[String]):Unit= {

    val sc = new SparkConf()
    sc.set("spark.app.name", "SPark Df")
    sc.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sc)
      .getOrCreate()

    import spark.implicits._
    val sales = List(
      (1, 100),
      (2, 1500),
      (3, 300)
    ).toDF("sale_id", "amount")
//    Question: How would you add a new column discount with values 0 if amount is less than 200, 10 if
//    amount is between 200 and 1000, and 20 if amount is greater than 1000?

    sales.select(col("sale_id"),col("amount"),
      when(col("amount")<200,"0").when(col("amount")>200 && col("amount")<1000,"10")
        .when(col("amount")>1000,"20")
       .alias("discount")).show()


  }

}
