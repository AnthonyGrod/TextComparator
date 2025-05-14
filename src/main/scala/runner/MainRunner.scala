package runner

import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import _root_.hash.HashFunctionsHolder
import config.Config
import shingle.Shingler
import util.MatrixUtil

import scala.util.hashing.MurmurHash3


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
      .rdd
      .map(row => (row.getString(1), row.getInt(0)))

    val entries: RDD[MatrixEntry] = Shingler.createMatrixEntries(spark.sparkContext, questionTextsWithIndex)
    /** Now we want to create a signature matrix. */
    val signatureMatrix: RDD[(Array[Long], Int)] = MatrixUtil.createSignatureMatrix(
                                                          spark.sparkContext,
                                                          entries,
                                                          HashFunctionsHolder.hashFunctions)

    val bandHashes: RDD[((Int, Int), Int)] =
      signatureMatrix.flatMap { case (row, rowIndex) =>
        // Group the row into bands of Config.BAND_SIZE elements
        row.grouped(Config.BAND_SIZE).zipWithIndex.map { case (band, bandIndex) =>
          val bandHash = MurmurHash3.arrayHash(band, Config.SEEDS(bandIndex))
          ((bandIndex, bandHash), rowIndex)
        }
      }.repartition(spark.sparkContext.defaultParallelism * 2)

    val duplicateBands: RDD[((Int, Int), Iterable[Int])] = bandHashes.groupByKey().repartition(spark.sparkContext.defaultParallelism * 2)

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

    val truePositives: RDD[(Long, Long)] = candidatePairs
      .intersection(truePositiveGold)
      .repartition(spark.sparkContext.defaultParallelism * 2)

    val numTruePositives = truePositives.count()

    val falsePositives: RDD[(Long, Long)] = candidatePairs
      .subtract(truePositiveGold)
      .intersection(trueNegativeGold)
      .repartition(spark.sparkContext.defaultParallelism * 2)
    val numFalsePositives = falsePositives.count()

    println(s"True Positives: $numTruePositives")
    println(s"False Positives: $numFalsePositives")

    spark.stop()
  }
}
