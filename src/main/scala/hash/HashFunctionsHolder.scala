package hash

import scala.util.Random
import config.Config

/**
 * A universal hash function implementation for LSH
 */
class UniversalHash(val a: Long, val b: Long, val p: Long) extends Serializable {
  def hash(x: Int): Long = {
    val xLong = x.toLong & 0xFFFFFFFFFFFFFFFFL
    val hashValue = ((a * xLong + b) % p)
    if (hashValue < 0) hashValue + Long.MaxValue else hashValue
  }
}

object UniversalHashGenerator {
  private val LARGE_PRIME: Long = 2305843009213693951L

  def generateHashFunctions(count: Int, masterSeed: Int = 42): Array[UniversalHash] = {
    val random = new Random(masterSeed)

    Array.fill(count) {
      val a = (random.nextLong() % (LARGE_PRIME - 1)) + 1
      val b = random.nextLong() % LARGE_PRIME
      new UniversalHash(a, b, LARGE_PRIME)
    }
  }
}

object HashFunctionsHolder {
  val hashFunctions: Array[UniversalHash] =
    UniversalHashGenerator.generateHashFunctions(Config.NUM_OF_HASH_FUN)
}