package shingle

import config.Config
import config.Config.MAX_SHINGLE_HASH
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.util.hashing.MurmurHash3

object Shingler {
  /**
   * Extracts shingles from a given text and converts them to integer hashes
   *
   * @param text Input text to extract shingles from
   * @param k    Size of each shingle (number of characters in each shingle)
   * @return A set of shingles as integer hash values
   */
  private def extractShingles(text: String, k: Int = Config.SHINGLE_SIZE): Set[Int] = {
    if (text == null || text.length < k) {
      Set.empty[Int]
    } else {
      val normalizedText = stripQuestionPrefixes(normalizeText(text.toLowerCase))

      normalizedText.sliding(k).map { shingle =>
        (MurmurHash3.stringHash(shingle) % MAX_SHINGLE_HASH) & 0x7FFFFFFF
      }.toSet
    }
  }

  def createMatrixEntries(sc: SparkContext, questionTextsRDD: RDD[(String, Int)]): RDD[MatrixEntry] = {
    questionTextsRDD.flatMap { case (text, docIndex) =>
      val shingles = extractShingles(text)

      shingles.map(shingleHash => MatrixEntry(docIndex, shingleHash, 1.0))
    }.repartition(sc.defaultParallelism * 2)
  }

  private def normalizeText(text: String): String = {
    val sb = new StringBuilder(text.length)
    var i = 0
    while (i < text.length) {
      val c = Character.toLowerCase(text.charAt(i))
      if (!Character.isWhitespace(c)) {
        sb.append(c)
      }
      i += 1
    }
    sb.toString
  }

  private def stripQuestionPrefixes(text: String): String = {
    val prefixes = Array("how", "what", "which", "why", "does")

    val trimmed = text.trim

    // Check if the text starts with any of the prefixes
    prefixes.foldLeft(trimmed) { (current, prefix) =>
      if (current.startsWith(prefix)) {
        // Remove the prefix and any space after it
        current.substring(prefix.length).trim
      } else {
        current
      }
    }
  }



}

