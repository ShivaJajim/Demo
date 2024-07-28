import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object DF11 {

  def main(args:Array[String]):Unit= {

    val sc = new SparkConf()
    sc.set("spark.app.name", "SPark Df")
    sc.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sc)
      .getOrCreate()

    import spark.implicits._

    val employees = List(
      (1, 25, 30000),
      (2, 45, 50000),
      (3, 35, 40000)
    ).toDF("employee_id", "age", "salary")
//    Question: How would you add a new column category with values "Young & Low Salary" if age is less
//    than 30 and salary is less than 35000, "Middle Aged & Medium Salary" if age is between 30 and 40
//    and salary is between 35000 and 45000, and "Old & High Salary" otherwise?

    employees.select(col("employee_id"),col("age"),col("salary"),
      when(col("age")< "30" && col("salary")< "35000"  ,"Young & Low Salary")
        .when((col("age")> "30" && col("age")< "40" ) && (col("salary")>= "35000" && col("salary")< "45000" )
          ,"Middle Aged & Medium Salary")
        .otherwise("Old & High Salary")
        .alias("category")).show(false)


  }

}
