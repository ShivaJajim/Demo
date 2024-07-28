import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.zookeeper.server.SessionTracker.Session

object Df1 {
//  1.Conditional Column: Data: A DataFrame employees with columns id, name, age.
//  val employees = List(
//    (1, "John", 28),
//    (2, "Jane", 35),
//    (3, "Doe", 22)
//  ).toDF("id", "name", "age")
//  Question: How would you add a new column is_adult which is true if the age is greater than or equal
//  to 18, and false otherwise?

  def main(args:Array[String]):Unit={

    val sc=new SparkConf()
      .set("spark.app.name","Spark DataFrame")
      .set("spark.master","local[*]")

    val sparks=SparkSession.builder()
      .config(sc)
      .getOrCreate()

    import sparks.implicits._

    val employees = List(
        (1, "John", 28),
        (2, "Jane", 35),
        (3, "Doe", 22)
      ).toDF("id", "name", "age")

    //  Question: How would you add a new column is_adult which is true if the age is greater than or equal
    //  to 18, and false otherwise?

    employees.select(col("id"),col("name"),col("age"),
    when(col("age")>= 18, "TRUE").otherwise("FALSE").alias("is_adult")).show()



  }

}
