package runner

import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.{DenseVector, SparseMatrix, SparseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import _root_.hash.HashFunctionsHolder
import config.Config
import config.Config.TEMPORARY_LIMIT
import org.apache.spark.sql.Encoders
import shingle.Shingler
import util.MatrixUtil

import scala.math.pow
import scala.util.hashing.MurmurHash3



object MainRunner {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("lsh-test-app")
      .master("local[*]")
      .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "5g")
      .config("spark.sql.shuffle.partitions", 25)
      .getOrCreate()


    val questionsDf: DataFrame = spark.read.format("csv")
      .load("data/assignment_questions.csv")
      .toDF("id", "question")
      .filter(row => row.getString(0) != "id" && row.getString(1) != "question")


    val goldDf: DataFrame = spark.read.format("csv")
      .load("data/assignment_gold_sample.csv")
      .toDF("qid1", "qid2", "is_duplicate")
      .filter(row => row.getString(0) != "qid1" && row.getString(1) != "qid2" && row.getString(2) != "is_duplicate")

    questionsDf.printSchema()

    val typedQuestionsDf = questionsDf.withColumn("id", questionsDf.col("id").cast("int"))
    val sortedQuestionsDf = typedQuestionsDf.orderBy("id").filter(col("id").isNotNull)

    val questionTextsRDD = sortedQuestionsDf.select("question")
      .limit(TEMPORARY_LIMIT) // TODO(in actual prod, remove limit)
      .rdd.map(row => row.getString(0))
    val numOfQuestions = questionTextsRDD.count()


    val entries: RDD[MatrixEntry] = Shingler.createMatrixEntries(spark.sparkContext, questionTextsRDD)
    entries.cache()
    println(s"Number of entries: ${entries.count()}")
//    val entries = Shingler.createBatchedMatrixEntries(spark.sparkContext, questionTextsRDD, 5000)
    val coordInputMatrix: CoordinateMatrix = new CoordinateMatrix(entries)

    /** Now we want to create a signature matrix. We will use 100 minhash functions. */
    val signatureMatrix: RDD[(Array[Int], Int)] = MatrixUtil.createSignatureMatrix(
                                                          spark.sparkContext,
                                                          coordInputMatrix.transpose(),
                                                          numOfQuestions.toInt,
                                                          Config.NUM_OF_HASH_FUN,
                                                          HashFunctionsHolder.hashFunctions)
//                                                          .transpose // rows: signatures
//    signatureMatrix.cache()
    println(s"Number of signatures: ${signatureMatrix.count()}")

    /** Now I have my signature matrix. Its dimensions are [HashFunctionsHolder.MINHASH_COUNT, numOfQuestions].
     * I want to partition this matrix into Config.NUM_OF_BANDS bands. Then, for b-th band of s-th signature,
     * I will hash each band and produce tuple ((b, hashValue), s). Then I will group by (b, hashValue) and find
     * all signatures that have their b-th band hashed to the same value.
     *
     * Then, create krotkas for golds: (sig1, sig2), where is_duplicates==True. Then transform krotkas above
     * (which are of type ((b, hashValue), listOfDuplicateSigs) into pairs (dup1, dup2), and take intersection
     * TRUE POSITIVES = |krotkas1 intersect krotkas2|
     * FALSE POSITIVES = |krotkas2| - |krotkas1 intersect krotkas2|
     */

    val bandHashes: RDD[((Int, Int), Int)] =
      signatureMatrix.flatMap { case (row, rowIndex) =>
        // Group the row into bands of Config.BAND_SIZE elements
        row.grouped(Config.BAND_SIZE).zipWithIndex.map { case (band, bandIndex) =>
          val bandHash = MurmurHash3.arrayHash(band, Config.SEEDS(bandIndex))
          ((bandIndex, bandHash), rowIndex)
        }
      }.repartition(spark.sparkContext.defaultParallelism * 2)

    // ((bandNumber, hashValue), signatureIndex)
    val duplicateBands: RDD[((Int, Int), Iterable[Int])] = bandHashes.groupByKey().repartition(spark.sparkContext.defaultParallelism * 2)

    val candidatePairs: RDD[(Int, Int)] = duplicateBands
      .filter { case (_, docIds) => docIds.size > 1 }
      .flatMap { case (_, docIds) =>
        val docIdArray = docIds.toArray

        for {
          i <- docIdArray.indices
          j <- i + 1 until docIdArray.length
          q1 = math.min(docIdArray(i), docIdArray(j)) + 1 // because matrix indexing if from 0 and qids are from 1
          q2 = math.max(docIdArray(i), docIdArray(j)) + 1
        } yield (q1, q2)
      }
      .repartition(spark.sparkContext.defaultParallelism * 2)
      .distinct()
      .cache()

//    println(s"Number of candidate pairs: ${candidatePairs.count()}")
//    println(s"Candidate pairs: ${candidatePairs.sortByKey().take(1500).mkString(", ")}")

    val typedGoldDf = goldDf
      .withColumn("qid1", goldDf.col("qid1").cast("int"))
      .withColumn("qid2", goldDf.col("qid2").cast("int"))
      .withColumn("is_duplicate", goldDf.col("is_duplicate").cast("boolean"))
      .filter(col("qid1") <= TEMPORARY_LIMIT && col("qid2") <= TEMPORARY_LIMIT) // TODO(in actual prod, remove limit)


    val goldRDD: RDD[(Int, Int, Boolean)] = typedGoldDf.select("qid1", "qid2", "is_duplicate").rdd.map { row =>
      val id1 = row.getInt(0)
      val id2 = row.getInt(1)
      (math.min(id1, id2), math.max(id1, id2), row.getBoolean(2))
    }

    val truePositiveGold: RDD[(Int, Int)] = goldRDD
      .filter { case (_, _, isDuplicate) => isDuplicate }
      .map { case (id1, id2, _) => (id1, id2) }

    // Find true positives (correctly identified duplicates)
    val truePositives: RDD[(Int, Int)] = candidatePairs.intersection(truePositiveGold).repartition(spark.sparkContext.defaultParallelism * 2).cache()
    val numTruePositives = truePositives.count()

    // Find false positives (incorrectly identified as duplicates)
    val falsePositives: RDD[(Int, Int)] = candidatePairs.subtract(truePositiveGold).repartition(spark.sparkContext.defaultParallelism * 2)
    val numFalsePositives = falsePositives.count()

    // Calculate precision (percentage of predictions that are correct)
    val precision = numTruePositives.toDouble / (numTruePositives + numFalsePositives)

    val trueGolds = goldRDD.filter { case (_, _, isDuplicate) => isDuplicate }

    println(s"True Positives: $numTruePositives")
    println(s"False Positives: $numFalsePositives")
    println(s"Total Predictions: ${candidatePairs.count()}")
    println(s"True Golds: ${trueGolds.count()}")
    println(s"Precision: $precision")
    println(s"Recall: ${numTruePositives.toDouble / trueGolds.count()}")

    spark.stop()
  }
}
