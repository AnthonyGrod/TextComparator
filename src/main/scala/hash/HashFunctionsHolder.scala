package hash

import scala.util.Random

object HashFunctionsHolder {
  val MINHASH_COUNT: Int = 100
  val hashFunctions: Array[TabulationHash] = TabulationHashGenerator.generateHashFunctions(MINHASH_COUNT)
}

/**
 * A tabulation hash function for 32-bit integers.
 *
 * @param seed The seed for random number generation
 */
class TabulationHash(val seed: Int) extends Serializable {
  // Create 4 tables for hashing 32-bit integers (one table per byte)
  private val tables: Array[Array[Int]] = Array.fill(4)(Array.fill(256)(0))

  // Initialize tables with random values based on seed
  {
    val random = new Random(seed)
    for {
      table <- tables
      i <- 0 until 256
    } {
      table(i) = random.nextInt()
    }
  }

  /**
   * Hash a 32-bit integer using tabulation hashing.
   *
   * @param x The integer to hash
   * @return The hash value
   */
  def hash(x: Int): Int = {
    // Extract each byte of the input
    val byte0 = (x & 0xFF)
    val byte1 = ((x >> 8) & 0xFF)
    val byte2 = ((x >> 16) & 0xFF)
    val byte3 = ((x >> 24) & 0xFF)

    // Combine the values from the tables using XOR
    tables(0)(byte0) ^ tables(1)(byte1) ^ tables(2)(byte2) ^ tables(3)(byte3)
  }
}

/**
 * Generator for multiple independent tabulation hash functions.
 */
private object TabulationHashGenerator {
  /**
   * Create an array of independent tabulation hash functions.
   *
   * @param count Number of hash functions to generate
   * @param masterSeed Base seed to derive individual hash function seeds
   * @return Array of independent tabulation hash functions
   */
  def generateHashFunctions(count: Int, masterSeed: Int = 42): Array[TabulationHash] = {
    val masterRandom = new Random(masterSeed)

    // Generate hash functions with different seeds derived from masterSeed
    Array.fill(count) {
      val seed = masterRandom.nextInt()
      new TabulationHash(seed)
    }
  }
}
