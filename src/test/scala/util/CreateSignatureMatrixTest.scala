//package util
//
//import org.apache.spark.SparkContext
//import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
//import org.scalatest.BeforeAndAfterAll
//import org.apache.spark.{SparkConf, SparkContext}
//import hash.HashFunctionsHolder
//import munit.FunSuite
//import org.scalatest.funsuite.AnyFunSuite
//import util.MatrixUtil
//import org.apache.spark.SparkContext
//import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
//import org.scalatest.BeforeAndAfterAll
//import org.apache.spark.{SparkConf, SparkContext}
//import hash.{HashFunctionsHolder, TabulationHash}
//import org.scalatest.funsuite.AnyFunSuite
//
//import scala.util.Random
//
//
//class CreateSignatureMatrixTest extends AnyFunSuite with BeforeAndAfterAll {
//
//  private var sc: SparkContext = _
//
//  override def beforeAll(): Unit = {
//    val conf = new SparkConf()
//      .setAppName("DistributedSignatureMatrixTest")
//      .setMaster("local[2]")
//      // Set log level to reduce output noise during tests
//      .set("spark.ui.enabled", "false")
//    sc = new SparkContext(conf)
//    sc.setLogLevel("ERROR")
//  }
//
//  override def afterAll(): Unit = {
//    if (sc != null) {
//      sc.stop()
//    }
//  }
//
//  test("createSignatureMatrix should handle empty matrix") {
//    val emptyEntries = Seq.empty[MatrixEntry]
//    val emptyMatrix = new CoordinateMatrix(sc.parallelize(emptyEntries))
//    val numDocuments = 5
//    val hashFunctionsCount = 10
//    val hashFunctions = generateHashFunctions(hashFunctionsCount)
//
//    val sigMatrix = MatrixUtil.createSignatureMatrix(
//      sc, emptyMatrix, numDocuments, hashFunctionsCount, hashFunctions
//    )
//
//    // Verify dimensions
//    assert(sigMatrix.length == hashFunctionsCount)
//    sigMatrix.foreach(row => assert(row.length == numDocuments))
//
//    // Verify all values remain at Int.MaxValue since no entries to process
//    sigMatrix.flatten.foreach(value => assert(value == Int.MaxValue))
//  }
//
//  test("createSignatureMatrix should work with small matrix") {
//    // Create a small test matrix with known values
//    val entries = Seq(
//      // Document 0 contains shingles 1, 3, 5
//      MatrixEntry(1, 0, 1.0),
//      MatrixEntry(3, 0, 1.0),
//      MatrixEntry(5, 0, 1.0),
//
//      // Document 1 contains shingles 2, 3, 4
//      MatrixEntry(2, 1, 1.0),
//      MatrixEntry(3, 1, 1.0),
//      MatrixEntry(4, 1, 1.0),
//
//      // Document 2 contains shingles 1, 4, 5
//      MatrixEntry(1, 2, 1.0),
//      MatrixEntry(4, 2, 1.0),
//      MatrixEntry(5, 2, 1.0)
//    )
//
//    val matrix = new CoordinateMatrix(sc.parallelize(entries))
//    val numDocuments = 3
//    val hashFunctionsCount = 20
//    val hashFunctions = generateHashFunctions(hashFunctionsCount)
//
//    // Generate the signature matrix
//    val sigMatrix = MatrixUtil.createSignatureMatrix(
//      sc, matrix, numDocuments, hashFunctionsCount, hashFunctions
//    )
//
//    // Verify dimensions
//    assert(sigMatrix.length == hashFunctionsCount)
//    sigMatrix.foreach(row => assert(row.length == numDocuments))
//
//    // Test expected properties
//    // 1. Documents with more elements in common should have more similar signatures
//    // Calculate Jaccard similarity between pairs of documents
//    val jaccard01 = calculateJaccard(Set(1, 3, 5), Set(2, 3, 4)) // Doc 0 and 1 share shingle 3
//    val jaccard02 = calculateJaccard(Set(1, 3, 5), Set(1, 4, 5)) // Doc 0 and 2 share shingles 1, 5
//    val jaccard12 = calculateJaccard(Set(2, 3, 4), Set(1, 4, 5)) // Doc 1 and 2 share shingle 4
//
//    // Calculate signature similarity (fraction of matching values)
//    val sigSim01 = calculateSignatureSimilarity(sigMatrix, 0, 1)
//    val sigSim02 = calculateSignatureSimilarity(sigMatrix, 0, 2)
//    val sigSim12 = calculateSignatureSimilarity(sigMatrix, 1, 2)
//
//    println(s"Jaccard similarities: 0-1: $jaccard01, 0-2: $jaccard02, 1-2: $jaccard12")
//    println(s"Signature similarities: 0-1: $sigSim01, 0-2: $sigSim02, 1-2: $sigSim12")
//
//    // 3. Verify at least some values have been updated from Int.MaxValue
//    val totalValues = sigMatrix.flatten.length
//    val updatedValues = sigMatrix.flatten.count(_ != Int.MaxValue)
//    assert(updatedValues > 0, s"No signature values were updated from Int.MaxValue")
//    println(s"Total values: $totalValues, Updated values: $updatedValues")
//  }
//
//  test("createSignatureMatrix should work with larger sparse matrix") {
//    // Define test parameters
//    val numShingles = 1000 // Number of possible shingles
//    val numDocuments = 100 // Number of documents
//    val sparsity = 0.01 // Only 1% of entries are non-zero
//    val hashFunctionsCount = 50
//    val hashFunctions = generateHashFunctions(hashFunctionsCount)
//
//    // Generate a larger sparse test matrix
//    val entries = generateSparseMatrix(numShingles, numDocuments, sparsity)
//    val matrix = new CoordinateMatrix(sc.parallelize(entries))
//
//    // Generate the signature matrix
//    val sigMatrix = MatrixUtil.createSignatureMatrix(
//      sc, matrix, numDocuments, hashFunctionsCount, hashFunctions
//    )
//
//    // Verify dimensions
//    assert(sigMatrix.length == hashFunctionsCount)
//    sigMatrix.foreach(row => assert(row.length == numDocuments))
//
//    // Verify that values were updated
//    val updatedValues = sigMatrix.flatten.count(_ != Int.MaxValue)
//    assert(updatedValues > 0, s"No signature values were updated from Int.MaxValue")
//    println(s"In large matrix test: Updated ${updatedValues} out of ${sigMatrix.flatten.length} values")
//
//    // Test signature properties on a few documents
//    val docPairs = for {
//      i <- 0 until 5
//      j <- i + 1 until 5
//    } yield (i, j)
//
//    // Compare minhash similarities between documents
//    docPairs.foreach { case (doc1, doc2) =>
//      val sim = calculateSignatureSimilarity(sigMatrix, doc1, doc2)
//      println(s"Signature similarity between docs $doc1 and $doc2: $sim")
//    }
//  }
//
//  test("createSignatureMatrix should be deterministic with fixed seeds") {
//    // Create a matrix with known values
//    val entries = Seq(
//      MatrixEntry(1, 0, 1.0),
//      MatrixEntry(3, 0, 1.0),
//      MatrixEntry(2, 1, 1.0),
//      MatrixEntry(3, 1, 1.0),
//      MatrixEntry(1, 2, 1.0),
//      MatrixEntry(4, 2, 1.0)
//    )
//
//    val matrix = new CoordinateMatrix(sc.parallelize(entries))
//    val numDocuments = 3
//    val hashFunctionsCount = 10
//
//    // Create hash functions with fixed seed
//    val fixedSeedHashFunctions = generateHashFunctions(hashFunctionsCount, 42)
//
//    // Generate the signature matrix twice
//    val sigMatrix1 = MatrixUtil.createSignatureMatrix(
//      sc, matrix, numDocuments, hashFunctionsCount, fixedSeedHashFunctions
//    )
//
//    val sigMatrix2 = MatrixUtil.createSignatureMatrix(
//      sc, matrix, numDocuments, hashFunctionsCount, fixedSeedHashFunctions
//    )
//
//    // Verify both matrices are identical
//    for (i <- 0 until hashFunctionsCount) {
//      for (j <- 0 until numDocuments) {
//        assert(sigMatrix1(i)(j) == sigMatrix2(i)(j),
//          s"Signatures differ at position ($i,$j): ${sigMatrix1(i)(j)} vs ${sigMatrix2(i)(j)}")
//      }
//    }
//  }
//
//  test("createSignatureMatrix should preserve MinHash property") {
//    // Test matrix with specific pattern - docs with more common shingles should have more similar signatures
//    val entries = Seq(
//      // Doc pairs (0,1) share 4 elements - should be most similar
//      MatrixEntry(1, 0, 1.0), MatrixEntry(2, 0, 1.0), MatrixEntry(3, 0, 1.0), MatrixEntry(4, 0, 1.0), MatrixEntry(5, 0, 1.0),
//      MatrixEntry(1, 1, 1.0), MatrixEntry(2, 1, 1.0), MatrixEntry(3, 1, 1.0), MatrixEntry(4, 1, 1.0), MatrixEntry(6, 1, 1.0),
//
//      // Doc pairs (0,2) share 2 elements - moderate similarity
//      MatrixEntry(1, 2, 1.0), MatrixEntry(2, 2, 1.0), MatrixEntry(7, 2, 1.0), MatrixEntry(8, 2, 1.0), MatrixEntry(9, 2, 1.0),
//
//      // Doc pairs (1,2) share 1 element - least similar
//      MatrixEntry(10, 3, 1.0), MatrixEntry(11, 3, 1.0), MatrixEntry(12, 3, 1.0), MatrixEntry(13, 3, 1.0), MatrixEntry(14, 3, 1.0)
//    )
//
//    val matrix = new CoordinateMatrix(sc.parallelize(entries))
//    val numDocuments = 4
//    val hashFunctionsCount = 100 // More hash functions for better MinHash approximation
//    val hashFunctions = generateHashFunctions(hashFunctionsCount)
//
//    // Generate the signature matrix
//    val sigMatrix = MatrixUtil.createSignatureMatrix(
//      sc, matrix, numDocuments, hashFunctionsCount, hashFunctions
//    )
//
//    // Calculate Jaccard similarities
//    val jaccard01 = calculateJaccard(Set(1, 2, 3, 4, 5), Set(1, 2, 3, 4, 6)) // 4/6 = 0.67
//    val jaccard02 = calculateJaccard(Set(1, 2, 3, 4, 5), Set(1, 2, 7, 8, 9)) // 2/8 = 0.25
//    val jaccard13 = calculateJaccard(Set(1, 2, 3, 4, 6), Set(10, 11, 12, 13, 14)) // 0/10 = 0.0
//
//    // Calculate signature similarities
//    val sigSim01 = calculateSignatureSimilarity(sigMatrix, 0, 1)
//    val sigSim02 = calculateSignatureSimilarity(sigMatrix, 0, 2)
//    val sigSim13 = calculateSignatureSimilarity(sigMatrix, 1, 3)
//
//    // Verify that relative ordering of similarities is preserved
//    println(s"Jaccard similarities: 0-1: $jaccard01, 0-2: $jaccard02, 1-3: $jaccard13")
//    println(s"Signature similarities: 0-1: $sigSim01, 0-2: $sigSim02, 1-3: $sigSim13")
//
//    // The MinHash property: signature similarities should approximate Jaccard similarities
//    assert(math.abs(sigSim01 - jaccard01) < 0.2, "MinHash approximation error too large for highly similar docs")
//    assert(math.abs(sigSim02 - jaccard02) < 0.2, "MinHash approximation error too large for moderately similar docs")
//    assert(math.abs(sigSim13 - jaccard13) < 0.2, "MinHash approximation error too large for dissimilar docs")
//
//    // Relative ordering should be preserved
//    assert(sigSim01 > sigSim02, "MinHash failed to preserve relative similarity ordering")
//    assert(sigSim02 > sigSim13, "MinHash failed to preserve relative similarity ordering")
//  }
//
//  // Helper method to generate hash functions
//  private def generateHashFunctions(count: Int, seed: Int = System.currentTimeMillis().toInt): Array[TabulationHash] = {
//    val random = new Random(seed)
//    Array.fill(count) {
//      new TabulationHash(random.nextInt())
//    }
//  }
//
//  // Helper method to generate a sparse matrix with random entries
//  private def generateSparseMatrix(numRows: Int, numCols: Int, sparsity: Double): Seq[MatrixEntry] = {
//    val random = new Random(42) // Fixed seed for reproducibility
//    val expectedEntries = (numRows * numCols * sparsity).toInt
//
//    // Generate random non-zero entries
//    (0 until expectedEntries).map { _ =>
//      val row = random.nextInt(numRows)
//      val col = random.nextInt(numCols)
//      MatrixEntry(row, col, 1.0)
//    }.distinct // Ensure no duplicates
//  }
//
//  // Helper method to calculate Jaccard similarity between two sets
//  private def calculateJaccard(set1: Set[Int], set2: Set[Int]): Double = {
//    val intersection = set1.intersect(set2).size
//    val union = set1.union(set2).size
//    intersection.toDouble / union
//  }
//
//  // Helper method to calculate similarity between two signature columns
//  private def calculateSignatureSimilarity(signatures: Array[Array[Int]], col1: Int, col2: Int): Double = {
//    val total = signatures.length
//    val matching = signatures.count(row => row(col1) == row(col2))
//    matching.toDouble / total
//  }
//
//}
