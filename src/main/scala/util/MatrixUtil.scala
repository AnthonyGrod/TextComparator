package util

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector, SparseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import _root_.hash.{HashFunctionsHolder, TabulationHash}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import shingle.Shingler

object MatrixUtil {
  /**
   * @param coordInputMatrix Dimensions [Number of possible shingles (2^32), Number of questions (~5x10^5)]
   * @return Signature matrix
   * TODO(create test suite)
   */
  def createSignatureMatrix(sc: SparkContext,
                            coordInputMatrix: CoordinateMatrix, // VERY sparse
                            numOfQuestions: Int,
                            hashFunctionsCount: Int,
                            hashFunctions: Array[TabulationHash]): Array[Array[Int]] = {
    val nonZeroEntriesByRow = coordInputMatrix.entries
      .map(entry => (entry.i, entry.j))
      .groupByKey()
      .cache()  // Cache this RDD as we'll use it multiple times

    // Create a broadcast variable for hash functions to avoid sending them to each task
    val broadcastHashFunctions = sc.broadcast(hashFunctions)

    // Initialize the signature matrix with Int.MaxValue
    val sigMatrix = Array.fill(hashFunctionsCount)(Array.fill(numOfQuestions)(Int.MaxValue))

    // Process each row in parallel and collect the minimum hash values
    val rowMinHashes: RDD[((Int, Int), Int)] = nonZeroEntriesByRow.flatMap { case (rowIndex, colIndices) =>
      // For each row, apply all hash functions
      val rowHashValues: Array[(Int, Int)] = broadcastHashFunctions.value.zipWithIndex.map { case (hashFunc, i) =>
        (i, hashFunc.hash(rowIndex.toInt))
      }

      // For each column in this row, generate (hashFuncIndex, colIndex, hashValue) tuples
      colIndices.flatMap { colIndex =>
        rowHashValues.map { case (hashFuncIndex, hashValue) =>
          ((hashFuncIndex, colIndex.toInt), hashValue)
        }
      }
    }

    // Group by (hashFuncIndex, colIndex) and find minimum hash value for each
    val minHashValues: Array[((Int, Int), Int)] = rowMinHashes
      .reduceByKey(math.min)
      .collect()

    // Update the signature matrix with the minimum hash values
    minHashValues.foreach { case ((hashFuncIndex, colIndex), minHashValue) =>
      if (colIndex < numOfQuestions && hashFuncIndex < hashFunctionsCount) {
        sigMatrix(hashFuncIndex)(colIndex) = minHashValue
      }
    }

    sigMatrix
  }
}




//    val rowsNum: Int = coordInputMatrix.numRows().toInt // very large, approx 2bilion
//    val sigMatrix: Array[Array[Int]] = Array.fill(hashFunctionsCount)(Array.fill(numOfQuestions)(Int.MaxValue))
//
//
//    for (rowIndex <- 0 until rowsNum) { // rowsNum = num of questions ~2bilion; for each row r
//      val hashValues: Array[Int] = hashFunctions.map(_.hash(rowIndex)) // for each hash function h_i calculate h_i(row)
////      val nonZeroColumnIndices = coordInputMatrix.entries.filter(entry => entry.i == rowIndex && entry.value != 0.0).map(_.j)
////      for (colIndex <- nonZeroColumnIndices) { // for each column c if c has 1 in row r
//      for (colIndex <- 0 until numOfQuestions) // not so big, approx 500k
//        for (i <- 0 until hashFunctionsCount) { // for each hash fun h_i
//          val hashValue = hashValues(i) // get h_i(row)
//          if (hashValue < sigMatrix(i)(colIndex.toInt)) {
//            sigMatrix(i)(colIndex) = hashValue
//          }
//        }
//      }

//def isHashValueSmallest(coordinateMatrix: CoordinateMatrix, hashValue: Int, rowIndex: Int, colIndex: Int): Boolean = {
//  // Check if given entry exists in the matrix. If not, then we assume its value is +INF, so we return true.
//  if (coordinateMatrix.entries.filter(entry => entry.i == rowIndex && entry.j == colIndex).count() == 0)
//    return true
//  val existingEntryValue: Int = coordinateMatrix.entries.filter(entry => entry.i == rowIndex && entry.j == colIndex).first().value.toInt
//  if (hashValue < existingEntryValue) true
//  else false
//}