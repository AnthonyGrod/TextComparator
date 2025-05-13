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
      val normalizedText = text.toLowerCase.replaceAll("\\s+", " ").trim

      normalizedText.sliding(k).map { shingle =>
        MurmurHash3.stringHash(shingle, 42)
      }.toSet
    }
  }

  def createMatrixEntries(sc: SparkContext, questionTextsRDD: RDD[String]): RDD[MatrixEntry] = {
    // First preserve document order by assigning indices
    val indexedRDD = questionTextsRDD.zipWithIndex()

    // Apply repartitioning to the results of processing (which is the most compute-intensive part)
    indexedRDD.flatMap { case (text, docIndex) =>
      // Extract shingles for this document
      val shingles = extractShingles(text)

      // Map each shingle to a MatrixEntry
      // docIndex becomes row, shingle hash becomes column
      shingles.map(shingleHash => MatrixEntry(docIndex, shingleHash, 1.0))
    }.repartition(sc.defaultParallelism * 2) // Repartition the larger result dataset for better downstream processing
  }



  /**
   * Creates shingle matrix entries in batches to reduce memory pressure
   * @param sc SparkContext
   * @param questionTextsRDD RDD of texts
   * @param batchSize Size of each processing batch
   * @return RDD of MatrixEntry objects
   */
  def createBatchedMatrixEntries(sc: SparkContext, questionTextsRDD: RDD[String], batchSize: Int = 10000): RDD[MatrixEntry] = {
    // Get total count of documents - this forces a Spark job
    val docCount = questionTextsRDD.count()

    // Calculate number of batches
    val numBatches = Math.ceil(docCount.toDouble / batchSize).toInt

    // Process in batches to reduce memory pressure
    val batchResults = (0 until numBatches).map { batchIndex =>
      val start = batchIndex * batchSize
      val end = Math.min(start + batchSize, docCount.toInt)

      // Extract batch using zipWithIndex and filter
      val batchRDD = questionTextsRDD.zipWithIndex()
        .filter { case (_, idx) => idx >= start && idx < end }

      // Process this batch
      batchRDD.flatMap { case (text, docIndex) =>
        if (text == null || text.length < Config.SHINGLE_SIZE) {
          Seq.empty[MatrixEntry]
        } else {
          val normalized = normalizeText(text)

          val shingles = collection.mutable.HashSet[Int]()
          val end = normalized.length - Config.SHINGLE_SIZE + 1
          for (i <- 0 until end) {
            val shingle = normalized.substring(i, i + Config.SHINGLE_SIZE)
            val hash = MurmurHash3.stringHash(shingle)
            shingles.add(hash)
          }

          shingles.map(hash => MatrixEntry(docIndex, hash, 1.0))
        }
      }
    }

    // Union all batch results
    sc.union(batchResults)
  }

  private def normalizeText(text: String): String = {
    val sb = new StringBuilder(text.length)
    var wasSpace = false
    var i = 0
    while (i < text.length) {
      val c = Character.toLowerCase(text.charAt(i))
      if (Character.isWhitespace(c)) {
        if (!wasSpace && sb.nonEmpty) {
          sb.append(' ')
          wasSpace = true
        }
      } else {
        sb.append(c)
        wasSpace = false
      }
      i += 1
    }

    // Trim trailing space if exists
    if (sb.nonEmpty && sb.last == ' ') {
      sb.deleteCharAt(sb.length - 1)
    }

    sb.toString
  }

  def createOptimizedMatrixEntries(sc: SparkContext, questionTextsRDD: RDD[String]): RDD[MatrixEntry] = {
    // Get the number of available cores
    val numCores = sc.defaultParallelism

    // Calculate optimal partition count - aim for at least 2-3 partitions per core
    val optimalPartitions = math.max(numCores * 3, questionTextsRDD.getNumPartitions)

    // Repartition if needed to ensure good parallelism
    val repartitionedRDD = if (questionTextsRDD.getNumPartitions < optimalPartitions) {
      questionTextsRDD.repartition(optimalPartitions)
    } else {
      questionTextsRDD
    }

    // Cache the repartitioned RDD to avoid recomputation
    val cachedRDD = repartitionedRDD.cache()

    // Process in a single parallel operation
    val result = cachedRDD.zipWithIndex().flatMap { case (text, docIndex) =>
      if (text == null || text.length < Config.SHINGLE_SIZE) {
        Seq.empty[MatrixEntry]
      } else {
        // Use a StringBuilder for better string manipulation performance
        val normalized = normalizeText(text)

        // Use a mutable HashSet for better performance when accumulating shingles
        val shingles = collection.mutable.HashSet[Int]()

        // Extract all shingles without creating intermediate collections
        val end = normalized.length - Config.SHINGLE_SIZE + 1
        for (i <- 0 until end) {
          val shingle = normalized.substring(i, i + Config.SHINGLE_SIZE)
          val hash = MurmurHash3.stringHash(shingle)
          shingles.add(hash)
        }

        // Convert to MatrixEntry objects
        shingles.map(hash => MatrixEntry(docIndex, hash, 1.0))
      }
    }

    // Uncache the input RDD if we're done with it
    cachedRDD.unpersist()

    result
  }



}

