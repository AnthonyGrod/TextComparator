package util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import _root_.hash.UniversalHash
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.storage.StorageLevel


object MatrixUtil {
  def createSignatureMatrix(sc: SparkContext,
                            coordInputMatrix: CoordinateMatrix, // VERY sparse
                            hashFunctions: Array[UniversalHash]
                           ): RDD[(Array[Long], Int)] = {
    val optimalPartitions = Math.max(sc.defaultParallelism * 2,
      Math.min(1000, coordInputMatrix.numCols().toInt / 1000))

    println(s"Creating signature matrix with $optimalPartitions partitions")

    val broadcastHashFunctions = sc.broadcast(hashFunctions)
    val numHashFunctions = hashFunctions.length

    // Group matrix entries by rows (questions)
    val rowEntries = coordInputMatrix.entries
      .map(entry => (entry.i.toInt, entry.j.toInt))
      .groupByKey(optimalPartitions)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    println(s"Created column-grouped entries: ${rowEntries.count()} columns")

    val signatures = rowEntries.map { case (rowIndex, nonZeroColIndexes) =>
      val signature = Array.fill[Long](numHashFunctions)(Long.MaxValue)
      val hashFuncs = broadcastHashFunctions.value

      // For each row (shingle) in this column
      nonZeroColIndexes.foreach { colIndex =>
        // Apply all hash functions and update signature
        var i = 0
        while (i < numHashFunctions) {
          val hash = hashFuncs(i).hash(colIndex)
          if (hash < signature(i)) {
            signature(i) = hash
          }
          i += 1
        }
      }

      // Return the row's signature
      (signature, rowIndex)
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    rowEntries.unpersist()

    signatures
  }

}
