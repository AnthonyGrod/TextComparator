package shingle

import config.Config
import config.Config.MAX_SHINGLE_HASH
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}

import scala.util.hashing.MurmurHash3

object Shingler {
  /**
   * Extracts shingles from a given text and converts them to integer hashes
   * @param text Input text to extract shingles from
   * @param k Size of each shingle (number of characters in each shingle)
   * @return A set of shingles as integer hash values
   */
  private def extractShingles(text: String, k: Int = Config.SHINGLE_SIZE): Set[Int] = {
    if (text == null || text.length < k) {
      Set.empty[Int]
    } else {
      val normalizedText = text.toLowerCase.replaceAll("\\s+", " ").trim

      normalizedText.sliding(k).map { shingle =>
        MurmurHash3.stringHash(shingle)
      }.toSet
    }
  }

  def createSparseShingleEncodedVector(text: String): SparseVector = {
    val shingles = extractShingles(text)
    val indices: Array[Int] = shingles.toArray
    val values: Array[Double] = new Array[Double](shingles.size).map(_ => 1.0)
    new SparseVector(MAX_SHINGLE_HASH, indices, values)
  }
}
