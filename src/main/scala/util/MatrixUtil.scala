package util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import hash.UniversalHash
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.storage.StorageLevel


object MatrixUtil {
  def createSignatureMatrix(sc: SparkContext,
                            entries: RDD[MatrixEntry],
                            hashFunctions: Array[UniversalHash]
                           ): RDD[(Array[Long], Int)] = {
    val optimalPartitions = Math.max(sc.defaultParallelism * 2, 1000)

    val broadcastHashFunctions = sc.broadcast(hashFunctions)
    val numHashFunctions = hashFunctions.length

    // Group matrix entries by rows (questions)
    val rowEntries = entries
      .map(entry => (entry.i.toInt, entry.j.toInt))
      .groupByKey(optimalPartitions)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

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
