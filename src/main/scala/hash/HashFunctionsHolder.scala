package hash

import scala.util.Random
import config.Config

/**
 * A universal hash function implementation for LSH
 * h(x) = ((a * x + b) % p) % Int.MaxValue
 */
class UniversalHash(val a: Long, val b: Long, val p: Long) extends Serializable {
  /**
   * Apply the hash function to an integer value
   * @param x input value
   * @return hash value (always positive)
   */
  def hash(x: Int): Int = {
    // Ensure positive input (important for consistent hashing)
    val xLong = x.toLong & 0xFFFFFFFFL

    // Apply the universal hash function: ((a*x + b) % p) 
    val hashValue = ((a * xLong + b) % p).toInt

    // Ensure positive output
    if (hashValue < 0) hashValue + Int.MaxValue else hashValue
  }
}

/**
 * Factory for generating multiple universal hash functions
 */
object UniversalHashGenerator {
  // Large prime number for universal hashing
  // Mersenne prime 2^31 - 1 which is close to Int.MaxValue
  private val LARGE_PRIME: Long = 2147483647L

  /**
   * Create an array of independent universal hash functions
   *
   * @param count Number of hash functions to generate
   * @param masterSeed Seed for random number generation
   * @return Array of hash functions
   */
  def generateHashFunctions(count: Int, masterSeed: Int = 42): Array[UniversalHash] = {
    val random = new Random(masterSeed)

    Array.fill(count) {
      // Generate random coefficients
      // 'a' should be non-zero
      val a = (random.nextLong() % (LARGE_PRIME - 1)) + 1
      // 'b' can be any value
      val b = random.nextLong() % LARGE_PRIME

      new UniversalHash(a, b, LARGE_PRIME)
    }
  }
}

/**
 * Holder for the hash functions needed by the application 
 */
object HashFunctionsHolder {
  // Replace TabulationHash with UniversalHash
  val hashFunctions: Array[UniversalHash] =
    UniversalHashGenerator.generateHashFunctions(Config.NUM_OF_HASH_FUN)
}