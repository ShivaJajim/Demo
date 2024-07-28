import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.zookeeper.server.SessionTracker.Session

object DF2 {

//  2)Categorizing Values: Data: A DataFrame grades with columns student_id, score.
//  val grades = List(
//    (1, 85),
//    (2, 42),
//    (3, 73)
//  ).toDF("student_id", "score")
//  Question: How would you add a new column grade with values "Pass" if score is greater than or
//  equal to 50, and "Fail" otherwise?
  def main(args:Array[String]):Unit={
    val sc=new SparkConf()
    sc.set("spark.app.name","SParak DataFrame")
    sc.set("spark.master","local[*]")

    val sparks=SparkSession.builder()
      .config(sc)
      .getOrCreate()

    import sparks.implicits._

      val grades = List(
        (1, 85),
        (2, 42),
        (3, 73)
      ).toDF("student_id", "score")



    //  Question: How would you add a new column grade with values "Pass" if score is greater than or
    //  equal to 50, and "Fail" otherwise?

    grades.select(col("student_id"),col("score"),
      when(col("score") >= 50 ,"Pass").otherwise("Faile").alias("Grade")).show()



  }

}
