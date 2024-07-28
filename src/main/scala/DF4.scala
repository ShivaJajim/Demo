import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object DF4 {

  def main(args:Array[String]):Unit= {

    val sc = new SparkConf()
    sc.set("spark.app.name", "SPark Df")
    sc.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sc)
      .getOrCreate()

    import spark.implicits._
    val products = List(
      (1, 30.5),
      (2, 150.75),
      (3, 75.25)
    ).toDF("product_id", "price")

//    Question: How would you add a new column price_range with values "Cheap" if price is less than 50,
//    "Moderate" if price is between 50 and 100, and "Expensive" otherwise?

   products.select(col("price"),when(col("price")<50,"Cheap").when(col("price")>50 && col("price")<100 ,"Moderate")
     .otherwise("Expensive").alias("price_range")).show()


  }

}
