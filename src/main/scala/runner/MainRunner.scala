package runner

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import _root_.hash.HashFunctionsHolder
import config.Config
import config.Config.TEMPORARY_LIMIT
import shingle.Shingler
import util.MatrixUtil

case class BandKey(array: Array[Long], index: Int) extends Serializable {
  override def equals(obj: Any): Boolean = obj match {
    case BandKey(otherArray, otherIndex) =>
      otherIndex == index && java.util.Arrays.equals(array, otherArray)
    case _ => false
  }

  override def hashCode(): Int = {
    31 * java.util.Arrays.hashCode(array) + index.hashCode()
  }
}


object MainRunner {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("lsh-test-app")
      .master("local[*]")
      .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "5g")
      .config("spark.sql.shuffle.partitions", 25)
      .config("spark.ui.enabled", "true")
      .config("spark.driver.host", "0.0.0.0")
      .config("spark.ui.port", "4040")
      .getOrCreate()


    val questionsDf: DataFrame = spark.read.format("csv")
      .load("data/assignment_questions.csv")
      .toDF("id", "question")
      .filter(row => row.getString(0) != "id" && row.getString(1) != "question")


    val goldDf: DataFrame = spark.read.format("csv")
      .load("data/assignment_gold_sample.csv")
      .toDF("qid1", "qid2", "is_duplicate")
      .filter(row => row.getString(0) != "qid1" && row.getString(1) != "qid2" && row.getString(2) != "is_duplicate")

    val typedQuestionsDf = questionsDf.withColumn("id", questionsDf.col("id").cast("int"))
    val sortedQuestionsDf = typedQuestionsDf.filter(col("id").isNotNull).orderBy("id")

    val questionTextsWithIndex = sortedQuestionsDf
      .select("id", "question")
//      .limit(TEMPORARY_LIMIT)
      .rdd
      .map(row => (row.getString(1), row.getInt(0)))

    val entries: RDD[MatrixEntry] = Shingler.createMatrixEntries(spark.sparkContext, questionTextsWithIndex)
    entries.cache()
    println(s"Number of entries: ${entries.count()}")
    val coordInputMatrix: CoordinateMatrix = new CoordinateMatrix(entries)
    /** Now we want to create a signature matrix. */
    val signatureMatrix: RDD[(Array[Long], Int)] = MatrixUtil.createSignatureMatrix(
                                                          spark.sparkContext,
                                                          coordInputMatrix,
                                                          HashFunctionsHolder.hashFunctions)

    val bandHashes: RDD[((Long, Long), Int)] =
      signatureMatrix.flatMap { case (row, rowIndex) =>
        // Group the row into bands of Config.BAND_SIZE elements
        row.grouped(Config.BAND_SIZE).zipWithIndex.map { case (band, bandIndex) =>
//          val bandHash1Long = band.foldLeft(42L + Config.SEEDS(bandIndex)) { (hash, value) =>
//            (hash * 31) ^ (value & 0xFFFFFFFFFFFFFFFFL)
//          }
//
//          val bandHash2Long = band.foldRight(72L + Config.SEEDS(bandIndex)) { (hash, value) =>
//            (hash * 31) ^ (value & 0xFFFFFFFFFFFFFFFFL)
//          }
//
//          ((bandIndex.toLong, bandHash1Long, bandHash2Long), rowIndex)
          val bandHash: Long = (31 * java.util.Arrays.hashCode(band) + bandIndex.hashCode()) & 0xFFFFFFFFFFFFFFFFL
          ((bandIndex.toLong, bandHash), rowIndex)
        }
      }.repartition(spark.sparkContext.defaultParallelism * 2)

    println(s"Number of bandHashes: ${bandHashes.count()}")


    // ((bandNumber, hashValue1, hashValue2), signatureIndexes)
    val duplicateBands: RDD[((Long, Long), Iterable[Int])] = bandHashes.groupByKey().repartition(spark.sparkContext.defaultParallelism * 2)

    val candidatePairs: RDD[(Long, Long)] = duplicateBands
      .filter { case (_, docIds) => docIds.size > 1 }
      .flatMap { case (_, docIds) =>
        val docIdArray = docIds.toArray

        for {
          i <- docIdArray.indices
          j <- i + 1 until docIdArray.length
          q1 = math.min(docIdArray(i), docIdArray(j))
          q2 = math.max(docIdArray(i), docIdArray(j))
        } yield (q1.toLong, q2.toLong)
      }
      .repartition(spark.sparkContext.defaultParallelism * 2)
      .distinct()
      .cache()

    val typedGoldDf = goldDf
      .withColumn("qid1", goldDf.col("qid1").cast("int"))
      .withColumn("qid2", goldDf.col("qid2").cast("int"))
      .withColumn("is_duplicate", goldDf.col("is_duplicate").cast("boolean"))
//      .filter(col("qid1") <= TEMPORARY_LIMIT && col("qid2") <= TEMPORARY_LIMIT)

    val goldRDD: RDD[(Long, Long, Boolean)] = typedGoldDf.select("qid1", "qid2", "is_duplicate").rdd.map { row =>
      val id1 = row.getInt(0)
      val id2 = row.getInt(1)
      (math.min(id1, id2), math.max(id1, id2), row.getBoolean(2))
    }

    val truePositiveGold: RDD[(Long, Long)] = goldRDD
      .filter { case (_, _, isDuplicate) => isDuplicate }
      .map { case (id1, id2, _) => (id1, id2) }

    val trueNegativeGold: RDD[(Long, Long)] = goldRDD
      .filter { case (_, _, isDuplicate) => !isDuplicate }
      .map { case (id1, id2, _) => (id1, id2) }

    val truePositives: RDD[(Long, Long)] = candidatePairs.intersection(truePositiveGold).repartition(spark.sparkContext.defaultParallelism * 2).cache()
    val numTruePositives = truePositives.count()

    val falsePositives: RDD[(Long, Long)] = candidatePairs.subtract(truePositiveGold).intersection(trueNegativeGold).repartition(spark.sparkContext.defaultParallelism * 2).cache()
    val numFalsePositives = falsePositives.count()

    val precision = numTruePositives.toDouble / (numTruePositives + numFalsePositives)

    val trueGolds = goldRDD.filter { case (_, _, isDuplicate) => isDuplicate }

    println(s"True Positives: $numTruePositives")
    println(s"False Positives: $numFalsePositives")
    println(s"Total Predictions: ${candidatePairs.count()}")
    println(s"True Golds: ${trueGolds.count()}")
    println(s"Recall: ${numTruePositives.toDouble / trueGolds.count()}")
    println(s"Precision: $precision")

    spark.stop()
  }
}
