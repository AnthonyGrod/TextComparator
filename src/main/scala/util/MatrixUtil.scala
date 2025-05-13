package util

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector, SparseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import _root_.hash.{HashFunctionsHolder, TabulationHash}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.storage.StorageLevel
import shingle.Shingler

import scala.util.hashing.MurmurHash3

object MatrixUtil {
  /**
   * @param coordInputMatrix Dimensions [Number of possible shingles (2^32), Number of questions (~5x10^5)]
   * @return Signature matrix
   * TODO(create test suite)
   */
//  def createSignatureMatrix(sc: SparkContext,
//                            coordInputMatrix: CoordinateMatrix, // VERY sparse
//                            numOfQuestions: Int,
//                            hashFunctionsCount: Int,
//                            hashFunctions: Array[TabulationHash]): RDD[(Array[Int], Int)] = { // result will be given by columns (so by signatures)
//    println("Entered createSignatureMatrix")
//    val nonZeroEntriesByRow = coordInputMatrix.entries
//      .map(entry => (entry.i, entry.j))
//      .groupByKey()
//      .repartition(sc.defaultParallelism * 2)
//      .cache()
//
//    println(s"nonZeroEntriesByRow: ${nonZeroEntriesByRow.count()}")
//
//    println("Finished groupBy")
//
//    // Create a broadcast variable for hash functions to avoid sending them to each task
//    val broadcastHashFunctions = sc.broadcast(hashFunctions)
//
//    println("defaultParallelism: " + sc.defaultParallelism)
//
//    val rowColPairs: RDD[(Int, Int)] = nonZeroEntriesByRow.flatMap { case (rowIndex, colIndices) =>
//      colIndices.map(colIndex => (rowIndex.toInt, colIndex.toInt))
//    }.repartition(sc.defaultParallelism * 2).cache()
//
//    println(s"rowColPairs: ${rowColPairs.count()}")
//
//    val initialRDD = rowColPairs.map { case (rowIndex, colIndex) =>
//      (rowIndex, colIndex, broadcastHashFunctions.value.map(_.hash(rowIndex)))
//    }.cache().repartition(sc.defaultParallelism * 2)
//
//    println(s"initialRDD: ${initialRDD.count()}")
//
//    // Phase 2: Explode the results (this separates the expensive operation from the shuffle)
//    val hashValuesRDD = initialRDD.flatMap { case (_, colIndex, hashValues) =>
//      hashValues.zipWithIndex.map { case (hashValue, hashFuncIndex) =>
//        ((hashFuncIndex, colIndex), hashValue)
//      }
//    }
//
//
//
//    println(s"hashValuesRDD: ${hashValuesRDD.count()}")
//
//    // Reduce by key to find minimum hash values
//    val minHashValues = hashValuesRDD.reduceByKey(Math.min).cache()
//
//    println(s"minHashValuesRDD: ${minHashValues.count()}")
//
//    val columnsWithIndex: RDD[(Array[Int], Int)] = minHashValues
//      .map { case ((hashFuncIndex, colIndex), hashValue) =>
//        (colIndex, (hashFuncIndex, hashValue))
//      }
//      .groupByKey()
//      .map { case (colIndex, hashValues) =>
//        val sortedHashValues = hashValues.toSeq
//          .sortBy(_._1)
//          .map(_._2)
//          .toArray
//        (sortedHashValues, colIndex)
//      }.repartition(sc.defaultParallelism * 2)
//
//    println(s"columnsWithIndex: ${columnsWithIndex.count()}")
//
//    columnsWithIndex.cache()
//  }

  def createSignatureMatrix(sc: SparkContext,
                                                        coordInputMatrix: CoordinateMatrix, // VERY sparse
                                                        numOfQuestions: Int,
                                                        hashFunctionsCount: Int,
                                                        hashFunctions: Array[TabulationHash]
                                    ): RDD[(Array[Int], Int)] = {

    // Calculate optimal partition counts
    val optimalPartitions = Math.max(sc.defaultParallelism * 2,
      Math.min(1000, coordInputMatrix.numCols().toInt / 1000))

    println(s"Creating signature matrix with $optimalPartitions partitions")

    // Broadcast hash functions to avoid serialization overhead
    val broadcastHashFunctions = sc.broadcast(hashFunctions)
    val numHashFunctions = hashFunctions.length

    // Step 1: Group matrix entries by column
    // This avoids multiple repartitioning steps and directly prepares data for signature generation
    val columnEntries = coordInputMatrix.entries
      .map(entry => (entry.j.toInt, entry.i.toInt)) // (column, row)
      .groupByKey(optimalPartitions)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    println(s"Created column-grouped entries: ${columnEntries.count()} columns")

    // Step 2: Generate signatures in one pass without repeated transformations
    val signatures = columnEntries.map { case (colIndex, rowIndices) =>
      // Initialize signature array with max values
      val signature = Array.fill[Int](numHashFunctions)(Int.MaxValue)
      val hashFuncs = broadcastHashFunctions.value

      // For each row (shingle) in this column
      rowIndices.foreach { rowIndex =>
        // Apply all hash functions and update signature
        var i = 0
        while (i < numHashFunctions) {
          val hash = hashFuncs(i).hash(rowIndex)
          if (hash < signature(i)) {
            signature(i) = hash
          }
          i += 1
        }
      }

      // Return the column's signature
      (signature, colIndex)
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Unpersist the intermediate RDD
    columnEntries.unpersist()

    signatures
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