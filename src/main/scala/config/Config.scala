package config

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Config {
  val MAX_SHINGLE_HASH: Int = Int.MaxValue
  val NUM_OF_BANDS: Int = 20
  val NUM_OF_HASH_FUN: Int = 100
  val BAND_SIZE: Int = NUM_OF_HASH_FUN / NUM_OF_BANDS
  val SHINGLE_SIZE: Int = 5
  val TEMPORARY_LIMIT = 1000 // TODO(MAKE K SMALL (LIKE 2 OR 3) AND INTRODUCE DIFFERENT HASH FUNCS FOR BUCKETS)
  val SEEDS: Array[Int] = Array.tabulate(Config.NUM_OF_BANDS)(i => 42 + i)
  val SEEDS_SHINGLES: Array[Int] = Array.tabulate(Config.NUM_OF_HASH_FUN)(i => 42 + i)
  val PARALLELISM_LEVEL: Int = 100
}
