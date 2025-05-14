package config

object Config {
  val MAX_SHINGLE_HASH: Int = Int.MaxValue
  val NUM_OF_BANDS: Int = 100
  val NUM_OF_HASH_FUN: Int = 1000
  val BAND_SIZE: Int = NUM_OF_HASH_FUN / NUM_OF_BANDS
  val SHINGLE_SIZE: Int = 8
  val TEMPORARY_LIMIT = 1000000000
  val SEEDS: Array[Long] = Array.tabulate(Config.NUM_OF_BANDS)(i => 42 + i)
}
