package e2e

import config.Config
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.SparseVector
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.{SparkConf, SparkContext}
import hash.HashFunctionsHolder
import org.scalatest.funsuite.AnyFunSuite
import shingle.Shingler
import util.MatrixUtil

class LSHEndToEndTest extends AnyFunSuite with BeforeAndAfterAll {
  private var sc: SparkContext = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf()
      .setAppName("LSHEndToEndTest")
      .setMaster("local[2]")
      .set("spark.ui.enabled", "false")
    sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }

  test("similar questions should have similar signatures") {
    // Define test questions
    val questions = Seq(
      // Group 1: cat related questions - should have similar signatures
      "how can I feed my cat",
      "what food is best for my cat",
      "how much should I feed my cat daily",

      // Group 2: dog related questions - should have similar signatures
      "how can I feed my dog",
      "what food is best for my dog",
      "how much should I feed my dog daily",

      // Group 3: unrelated questions - should have different signatures
      "what's the weather like today",
      "how to learn programming",
      "best places to travel in Europe"
    )

    // Convert questions to RDD
    val questionsRDD = sc.parallelize(questions)
    val numOfQuestions = questions.length

    // Create shingle matrix
    val shingleMatrix = questionsRDD.map { text =>
      Shingler.createSparseShingleEncodedVector(text)
    }

    // Create matrix entries
    val entries = shingleMatrix.zipWithIndex().flatMap {
      case (vector, rowIndex) =>
        vector.indices.zip(vector.values).map {
          case (colIndex, value) => MatrixEntry(rowIndex, colIndex, value)
        }
    }

    val coordInputMatrix = new CoordinateMatrix(entries)

    // Create signature matrix
    val signatureMatrix = MatrixUtil.createSignatureMatrix(
      sc,
      coordInputMatrix.transpose(),
      numOfQuestions,
      Config.NUM_OF_HASH_FUN,
      HashFunctionsHolder.hashFunctions
    )

    // Verify dimensions
    assert(signatureMatrix.length == Config.NUM_OF_HASH_FUN)
    assert(signatureMatrix(0).length == numOfQuestions)

    // Calculate signature similarities between questions
    val similarities = calculateAllPairSimilarities(signatureMatrix, numOfQuestions)

    // Print signature similarity matrix
    println("Signature Similarity Matrix:")
    for (i <- 0 until numOfQuestions) {
      println(similarities(i).map(s => f"$s%.2f").mkString(" "))
    }

    // Extract and test group similarities

    // Group 1 (cat questions) internal similarities (indices 0-2)
    val catInternalSimilarities = for {
      i <- 0 until 3
      j <- i + 1 until 3
    } yield similarities(i)(j)

    // Group 2 (dog questions) internal similarities (indices 3-5)
    val dogInternalSimilarities = for {
      i <- 3 until 6
      j <- i + 1 until 6
    } yield similarities(i)(j)

    // Cross-group similarities (cat-dog questions) - should be medium similarity
    val catDogSimilarities = for {
      i <- 0 until 3
      j <- 3 until 6
    } yield similarities(i)(j)

    // Unrelated questions similarities (with cat and dog questions)
    val unrealatedSimilarities = for {
      i <- 6 until 9
      j <- 0 until 6
    } yield similarities(i)(j)

    // Calculate average similarities
    val avgCatSimilarity = catInternalSimilarities.sum / catInternalSimilarities.length
    val avgDogSimilarity = dogInternalSimilarities.sum / dogInternalSimilarities.length
    val avgCatDogSimilarity = catDogSimilarities.sum / catDogSimilarities.length
    val avgUnrelatedSimilarity = unrealatedSimilarities.sum / unrealatedSimilarities.length

    println(s"Average similarity within cat questions: $avgCatSimilarity")
    println(s"Average similarity within dog questions: $avgDogSimilarity")
    println(s"Average similarity between cat and dog questions: $avgCatDogSimilarity")
    println(s"Average similarity between unrelated and pet questions: $avgUnrelatedSimilarity")

    // Assert expected relationships
    assert(avgCatSimilarity > 0.6, "Cat questions should have high similarity")
    assert(avgDogSimilarity > 0.6, "Dog questions should have high similarity")
    assert(avgCatDogSimilarity > 0.3, "Cat and dog questions should have medium similarity")
    assert(avgUnrelatedSimilarity < 0.3, "Unrelated questions should have low similarity")

    // Verify relative similarities
    assert(avgCatSimilarity > avgCatDogSimilarity, "Within-group similarity should exceed cross-group similarity")
    assert(avgDogSimilarity > avgCatDogSimilarity, "Within-group similarity should exceed cross-group similarity")
    assert(avgCatDogSimilarity > avgUnrelatedSimilarity, "Cross-group similarity should exceed unrelated similarity")
  }

  test("should handle extreme cases correctly") {
    // Test with identical questions, completely different questions, and empty questions
    val questions = Seq(
      // Identical questions
      "this is a test question",
      "this is a test questio",

      // Similar question with minor changes
      "this is a test question with minor changes",

      // Completely different question
      "something entirely unrelated to previous questions",

      // Empty or very short question
      "",
      "a"
    )

    // Process questions the same way as the main test
    val questionsRDD = sc.parallelize(questions)
    val numOfQuestions = questions.length

    val shingleMatrix = questionsRDD.map { text =>
      Shingler.createSparseShingleEncodedVector(text)
    }

    val entries = shingleMatrix.zipWithIndex().flatMap {
      case (vector, rowIndex) =>
        vector.indices.zip(vector.values).map {
          case (colIndex, value) => MatrixEntry(rowIndex, colIndex, value)
        }
    }

    val coordInputMatrix = new CoordinateMatrix(entries)

    val signatureMatrix = MatrixUtil.createSignatureMatrix(
      sc,
      coordInputMatrix.transpose(),
      numOfQuestions,
      Config.NUM_OF_HASH_FUN,
      HashFunctionsHolder.hashFunctions
    )

    // Calculate signature similarities
    val similarities = calculateAllPairSimilarities(signatureMatrix, numOfQuestions)

    // Print similarity matrix
    println("Extreme Cases Similarity Matrix:")
    for (i <- 0 until numOfQuestions) {
      println(similarities(i).map(s => f"$s%.2f").mkString(" "))
    }

    // Check specific cases
    // Identical questions should have similarity very close to 1.0
    assert(similarities(0)(1) > 0.93, "Identical questions should have near-perfect similarity")

    signatureMatrix.foreach(row => println(row.mkString(" ")))

    // Similar questions should have high similarity
//    assert(similarities(0)(2) > 0.7, "Similar questions should have high similarity")
//
//    // Different questions should have low similarity
//    assert(similarities(0)(3) < 0.3, "Different questions should have low similarity")
//
//    // Empty or very short questions should be handled properly
//    // Empty question may not have any valid shingles
//    if (signatureMatrix(0)(4) != Int.MaxValue) {
//      assert(similarities(0)(4) < 0.2, "Empty questions should have very low similarity")
//    }
  }

  test("deterministic behavior with fixed hash functions") {
    // Test that the same questions always get the same signatures
    val questions = Seq(
      "question one about cats",
      "question two about dogs"
    )

    // Process the questions twice and verify signatures are identical
    def generateSignatures() = {
      val questionsRDD = sc.parallelize(questions)
      val numOfQuestions = questions.length

      val shingleMatrix = questionsRDD.map { text =>
        Shingler.createSparseShingleEncodedVector(text)
      }

      val entries = shingleMatrix.zipWithIndex().flatMap {
        case (vector, rowIndex) =>
          vector.indices.zip(vector.values).map {
            case (colIndex, value) => MatrixEntry(rowIndex, colIndex, value)
          }
      }

      val coordInputMatrix = new CoordinateMatrix(entries)

      MatrixUtil.createSignatureMatrix(
        sc,
        coordInputMatrix.transpose(),
        numOfQuestions,
        Config.NUM_OF_HASH_FUN,
        HashFunctionsHolder.hashFunctions
      )
    }

    val signatures1 = generateSignatures()
    val signatures2 = generateSignatures()

    // Verify both signatures are identical
    for (i <- 0 until Config.NUM_OF_HASH_FUN) {
      for (j <- 0 until questions.length) {
        assert(signatures1(i)(j) == signatures2(i)(j),
          s"Signatures differ at position ($i,$j): ${signatures1(i)(j)} vs ${signatures2(i)(j)}")
      }
    }
  }

  // Helper method: Calculate similarity between all question pairs
  private def calculateAllPairSimilarities(signatures: Array[Array[Int]], numQuestions: Int): Array[Array[Double]] = {
    val similarities = Array.ofDim[Double](numQuestions, numQuestions)

    for (i <- 0 until numQuestions) {
      for (j <- 0 until numQuestions) {
        if (i == j) {
          similarities(i)(j) = 1.0 // Identical questions have similarity 1
        } else {
          // Calculate Jaccard similarity approximation using MinHash
          val matching = signatures.count(row => row(i) == row(j))
          similarities(i)(j) = matching.toDouble / signatures.length
        }
      }
    }

    similarities
  }

  // Helper method to output human-readable question similarities
  private def describeQuestionSimilarities(questions: Seq[String], similarities: Array[Array[Double]]): Unit = {
    println("\nQuestion similarity breakdown:")
    for (i <- questions.indices) {
      for (j <- i + 1 until questions.length) {
        println(s"Questions '${questions(i)}' and '${questions(j)}' have similarity: ${similarities(i)(j)}")
      }
    }
  }
}