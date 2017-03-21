package stackoverflow

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  override def afterAll(): Unit = {
    import StackOverflow._
    sc.stop()
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }


  test("scored") {
    import StackOverflow._
    val lines = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored = scoredPostings(grouped)

    assert(scored.count() == 2121822, "Incorrect size")

    val idList = List(6, 42, 72, 126, 174)
    val result = scored.filter{case (posting, _) => idList.contains(posting.id)}.collect()

    assert(result.contains(Posting(1,6,None,None,140,Some("CSS")),67), "Does not contain CSS")
    assert(result.contains(Posting(1,42,None,None,155,Some("PHP")),89), "Does not contain PHP")
    assert(result.contains(Posting(1,72,None,None,16,Some("Ruby")),3), "Does not contain Ruby")
    assert(result.contains(Posting(1,126,None,None,33,Some("Java")),30), "Does not contain Java")
    assert(result.contains(Posting(1,174,None,None,38,Some("C#")),20), "Does not contain C#")
  }

  test("Vectors for clustering") {
    import StackOverflow._
    val lines = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored = scoredPostings(grouped)
    val vectors = vectorPostings(scored)

    assert(vectors.count() == 2121822, "Incorrect size")

    assert(vectors.filter(_ == (350000,67)).count() >= 1, "Does not contain (350000,67)")
    assert(vectors.filter(_ == (100000,89)).count() >= 1, "Does not contain (100000,89)")
    assert(vectors.filter(_ == (300000,3)).count() >= 1, "Does not contain (300000,3)")
    assert(vectors.filter(_ == (50000,30)).count() >= 1, "Does not contain (50000,30)")
    assert(vectors.filter(_ == (200000,20)).count() >= 1, "Does not contain (200000,20)")
  }
}
