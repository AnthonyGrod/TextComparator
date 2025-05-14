package config

object Config {
  val MAX_SHINGLE_HASH: Int = Int.MaxValue
  private val NUM_OF_BANDS: Int = 20
  val NUM_OF_HASH_FUN: Int = 100
  val BAND_SIZE: Int = NUM_OF_HASH_FUN / NUM_OF_BANDS
  val SHINGLE_SIZE: Int = 5
  val SEEDS: Array[Int] = Array.tabulate(Config.NUM_OF_BANDS)(i => 42 + i)
}
