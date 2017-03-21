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
    val raw = testObject.rawPostings(lines)
    val grouped = testObject.groupedPostings(raw)
    val scored = testObject.scoredPostings(grouped)
    val idList = List(6, 42, 72, 126, 174)
    val result = scored.filter{case (posting, _) => idList.contains(posting.id)}.collect()

    assert(result.contains((1,6,None,None,140,Some("CSS")),67), "(1,6,None,None,140,Some(\"CSS\")),67)")
    assert(result.contains((1,42,None,None,155,Some("PHP")),89), "(1,42,None,None,155,Some(\"PHP\")),89)")
    assert(result.contains((1,72,None,None,16,Some("Ruby")),3), "(1,72,None,None,16,Some(\"Ruby\")),3)")
    assert(result.contains((1,126,None,None,33,Some("Java")),30), "(1,126,None,None,33,Some(\"Java\")),30)")
    assert(result.contains((1,174,None,None,38,Some("C#")),20), "(1,174,None,None,38,Some(\"C#\")),20)")
  }
}
