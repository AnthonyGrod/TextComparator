package runner

import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.{DenseVector, SparseMatrix, SparseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import _root_.hash.HashFunctionsHolder
import org.apache.spark.sql.Encoders
import shingle.Shingler
import util.MatrixUtil

import scala.math.pow



object MainRunner {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("lsh-test-app").master("local[*]").getOrCreate()
    import spark.implicits._

    val questionsDf: DataFrame = spark.read.format("csv")
      .load("data/assignment_questions.csv")
      .toDF("id", "question")
      .filter(row => row.getString(0) != "id" && row.getString(1) != "question")
    questionsDf.show(10)
    val questionTextsRDD = questionsDf.select("question").rdd.map(row => row.getString(0))
    val numOfQuestions = questionTextsRDD.count()


    /**
     * Matrix represented by RDD of SparseVectors. shingleMatrix[i, j]=1 iff question i contains shingle j.
     */
    val shingleMatrix: RDD[SparseVector] = questionTextsRDD.map { text =>
      Shingler.createSparseShingleEncodedVector(text)
    }

    /**
     * Input matrix, where rows are sets and columns are shingles. That's why we'll need to transpose it when creating the signature matrix.
     */
    val entries: RDD[MatrixEntry] = shingleMatrix.zipWithIndex().flatMap {
      case (vector, rowIndex) =>
        vector.indices.zip(vector.values).map {
          case (colIndex, value) => MatrixEntry(rowIndex, colIndex, value)
        }
    }
    val coordInputMatrix: CoordinateMatrix = new CoordinateMatrix(entries)

    /** Now we want to create a signature matrix. We will use 100 minhash functions. */
    val signatureMatrix: Array[Array[Int]] = MatrixUtil.createSignatureMatrix(spark.sparkContext, coordInputMatrix.transpose(), numOfQuestions.toInt, HashFunctionsHolder.MINHASH_COUNT, HashFunctionsHolder.hashFunctions)



    spark.stop()
  }
}
