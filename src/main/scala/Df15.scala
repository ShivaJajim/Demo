import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object DF15 {

  def main(args:Array[String]):Unit= {

    val sc = new SparkConf()
    sc.set("spark.app.name", "SPark Df")
    sc.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sc)
      .getOrCreate()

    import spark.implicits._

    //5.Data: A DataFrame orders with columns order_id, quantity, total_price.
    val orders = List(
      (1, 5, 100),
      (2, 10, 150),
      (3, 20, 300)
    ).toDF("order_id", "quantity", "total_price")
//    Question: How would you add a new column order_type with values "Small & Cheap" if quantity is
//      less than 10 and total_price is less than 200, "Bulk & Discounted" if quantity is greater than or equal
//      to 10 and total_price is less than 200, and "Premium Order" otherwise?

    orders.select(col("order_id"),col("quantity"),col("total_price"),
      when(col("quantity")< "10" &&  col("total_price")< "200" ,"Small & Cheap")
        .when(col("quantity")>= "10" &&  col("total_price") <"200" ,"Bulk & Discounted")

        .otherwise("Premium Order")
        .alias("order_type")).show(false)


  }

}
