import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object DF3 {

  def main(args:Array[String]):Unit={

    val sc =new SparkConf()
    sc.set("spark.app.name","SPark Df")
    sc.set("spark.master","local[*]")

    val spark=SparkSession.builder()
      .config(sc)
      .getOrCreate()

    import spark.implicits._

    val transactions = List(
      (1, 1000),
      (2, 200),
      (3, 5000)
    ).toDF("transaction_id", "amount")

//    Question: How would you add a new column category with values "High" if amount is greater than
//      1000, "Medium" if amount is between 500 and 1000, and "Low" otherwise?

    transactions.select(col("transaction_id"),col("amount"),when(col("amount")>1000,"High")
    .when(col("amount")>500 && col("amount")<=1000,"Medium")
      .otherwise("Low").alias("category")).show()




  }




}
