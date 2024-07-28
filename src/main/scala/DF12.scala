import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object DF12 {

  def main(args:Array[String]):Unit= {

    val sc = new SparkConf()
    sc.set("spark.app.name", "SPark Df")
    sc.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sc)
      .getOrCreate()

    import spark.implicits._

    //2.Multiple Conditional Columns: Data: A DataFrame reviews with columns review_id, rating.
    val reviews = List(
      (1, 1),
      (2, 4),
      (3, 5)
    ).toDF("review_id", "rating")
//    Question: How would you add two new columns, feedback with values "Bad" if rating is less than 3,
//    "Good" if rating is 3 or 4, and "Excellent" if rating is 5, and is_positive with values true if rating is
//      greater than or equal to 3, and false otherwise?

    val df1=reviews.select(col("review_id"),col("rating"), when(col("rating")<3,"bad"). when(col("rating")<3 || col("rating")<4,"Good")
      .when(col("rating")===5,"Excellent")
        .otherwise("false")
        .alias("feedback")
    )
    val df2=reviews.select(col("review_id"),when(col("rating")>=3,"true").otherwise("false").alias("is_positive"))
    val df3= df1.join(df2,"review_id")
    df3.show()

  }

}
