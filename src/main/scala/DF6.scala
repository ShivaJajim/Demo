import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object DF6 {

  def main(args:Array[String]):Unit= {

    val sc = new SparkConf()
    sc.set("spark.app.name", "SPark Df")
    sc.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sc)
      .getOrCreate()

    import spark.implicits._
    val inventory = List(
      (1, 5),
      (2, 15),
      (3, 25)
    ).toDF("item_id", "quantity")

//    Question: How would you add a new column stock_level with values "Low" if quantity is less than 10,
//    "Medium" if quantity is between 10 and 20, and "High" otherwise?

    inventory.select(col("item_id"),col("quantity"),when(col("quantity") <10,"Low")
      .when(col("quantity") >10 && col("quantity") >20,"Medium")
      .otherwise("High").alias("stock_level")).show()


  }

}
