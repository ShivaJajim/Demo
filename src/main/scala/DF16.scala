import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object DF16 {

  def main(args:Array[String]):Unit= {

    val sc = new SparkConf()
    sc.set("spark.app.name", "SPark Df")
    sc.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sc)
      .getOrCreate()

    import spark.implicits._

//    6. Conditional Column with Multiple Boolean Values: Data: A DataFrame weather with columns
//    day_id, temperature, humidity.
    val weather = Seq(
      (1, 25, 60),
      (2, 35, 40),
      (3, 15, 80)
    ).toDF("day_id", "temperature", "humidity")
//    Question: How would you add two new columns, is_hot with values true if temperature is greater
//    than 30, and false otherwise, and is_humid

    val df1=weather.select(col("day_id"),col("temperature"),col("humidity"),
      when(col("temperature") > "30"  ,"true")
      .otherwise("False")
        .alias("is_hot"))
    val df2= weather.select(col("day_id"),
      when(col("humidity") > "50", "true")
        .otherwise("False")
        .alias("is_humid"))

    val df3=df1.join(df2,"day_id")
     df3.show(false)


  }

}
