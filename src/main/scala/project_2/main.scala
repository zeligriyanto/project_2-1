package project_2

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._

object main {

  val seed = new java.util.Date().hashCode
  val rand = new scala.util.Random(seed)

  class hash_function(numBuckets_in: Long) extends Serializable {
    val p: Long = 2147483587
    val a: Long = (rand.nextLong % (p - 1)) + 1
    val b: Long = (rand.nextLong % p)
    val numBuckets: Long = numBuckets_in

    def convert(s: String, ind: Int): Long = {
      if (ind == 0) return 0
      (s(ind - 1).toLong + 256 * convert(s, ind - 1)) % p
    }

    def hash(s: String): Long = ((a * convert(s, s.length) + b) % p) % numBuckets

    def hash(t: Long): Long = ((a * t + b) % p) % numBuckets

    def zeroes(num: Long, remain: Long): Int = {
      if ((num & 1) == 1 || remain == 1) 0 else 1 + zeroes(num >> 1, remain >> 1)
    }

    def zeroes(num: Long): Int = zeroes(num, numBuckets)
  }

  class four_universal_Radamacher_hash_function extends hash_function(2) {
    override val a: Long = (rand.nextLong % p)
    override val b: Long = (rand.nextLong % p)
    val c: Long = (rand.nextLong % p)
    val d: Long = (rand.nextLong % p)

    override def hash(s: String): Long = {
      val t = convert(s, s.length)
      val t2 = t * t % p
      val t3 = t2 * t % p
      if ((((a * t3 + b * t2 + c * t + b) % p) & 1) == 0) 1 else -1
    }

    override def hash(t: Long): Long = {
      val t2 = t * t % p
      val t3 = t2 * t % p
      if ((((a * t3 + b * t2 + c * t + b) % p) & 1) == 0) 1 else -1
    }
  }

  class BJKSTSketch(bucket_in: Set[(String, Int)], z_in: Int, bucket_size_in: Int) extends Serializable {
    var bucket: Set[(String, Int)] = bucket_in
    var z: Int = z_in
    val BJKST_bucket_size = bucket_size_in

    def this(s: String, z_of_s: Int, bucket_size_in: Int) {
      this(Set((s, z_of_s)), z_of_s, bucket_size_in)
    }

    def +(that: BJKSTSketch): BJKSTSketch = {
      if (this.bucket.isEmpty) return that
      if (that.bucket.isEmpty) return this

      val merged_bucket = this.bucket ++ that.bucket
      var merged_z = math.max(this.z, that.z)
      var filtered_bucket = merged_bucket.filter(_._2 >= merged_z)

      while (filtered_bucket.size > BJKST_bucket_size && filtered_bucket.nonEmpty) {
        merged_z += 1
        filtered_bucket = filtered_bucket.filter(_._2 >= merged_z)
      }

      new BJKSTSketch(filtered_bucket, merged_z, BJKST_bucket_size)
    }

    def add_string(s: String, z_of_s: Int): BJKSTSketch = {
      if (z_of_s >= this.z) {
        val new_bucket = this.bucket + ((s, z_of_s))
        if (new_bucket.size <= BJKST_bucket_size) {
          new BJKSTSketch(new_bucket, this.z, BJKST_bucket_size)
        } else {
          var new_z = this.z + 1
          var filtered_bucket = new_bucket.filter(_._2 >= new_z)
          while (filtered_bucket.size > BJKST_bucket_size && filtered_bucket.nonEmpty) {
            new_z += 1
            filtered_bucket = filtered_bucket.filter(_._2 >= new_z)
          }
          new BJKSTSketch(filtered_bucket, new_z, BJKST_bucket_size)
        }
      } else {
        this
      }
    }

    def estimate(): Double = {
      if (bucket.isEmpty) 0.0 else bucket.size * math.pow(2, z)
    }
  }

  def tidemark(x: RDD[String], trials: Int): Double = {
    val h = Seq.fill(trials)(new hash_function(2000000000))
    def param0 = (accu1: Seq[Int], accu2: Seq[Int]) => Seq.range(0, trials).map(i => math.max(accu1(i), accu2(i)))
    def param1 = (accu1: Seq[Int], s: String) => Seq.range(0, trials).map(i => math.max(accu1(i), h(i).zeroes(h(i).hash(s))))
    val x3 = x.aggregate(Seq.fill(trials)(0))(param1, param0)
    val ans = x3.map(z => math.pow(2, 0.5 + z)).sorted.apply(trials / 2)
    ans
  }

  def BJKST(x: RDD[String], width: Int, trials: Int): Double = {
    val hash_functions = Array.fill(trials)(new hash_function(1L << 32))
    val estimates = (0 until trials).map { trial =>
      val h = hash_functions(trial)
      val initialSketch = new BJKSTSketch(Set.empty[(String, Int)], 0, width)
      val result = x.aggregate(initialSketch)(
        (sketch, s) => sketch.add_string(s, h.zeroes(h.hash(s))),
        (s1, s2) => s1 + s2
      )
      val estimate = result.estimate()
      if (estimate < 1.0 && result.bucket.nonEmpty) 1.0 else estimate
    }
    val sorted_estimates = estimates.sorted
    if (trials % 2 == 0) (sorted_estimates(trials / 2) + sorted_estimates(trials / 2 - 1)) / 2.0
    else sorted_estimates(trials / 2)
  }

  def Tug_of_War(x: RDD[String], width: Int, depth: Int): Long = {
    val hash_functions = Array.fill(depth, width)(new four_universal_Radamacher_hash_function())
    val frequencies = x.map(s => (s, 1L)).reduceByKey(_ + _).cache()
    val sketches = Array.ofDim[Long](depth, width)

    for (d <- 0 until depth) {
      for (w <- 0 until width) {
        val sketch_value = frequencies.map {
          case (s, freq) => hash_functions(d)(w).hash(s) * freq
        }.reduce(_ + _)
        sketches(d)(w) = sketch_value
      }
    }

    val means = sketches.map(row => row.map(x => x * x).sum.toDouble / width)
    val sorted_means = means.sorted
    val median = if (depth % 2 == 0) (sorted_means(depth / 2) + sorted_means(depth / 2 - 1)) / 2.0 else sorted_means(depth / 2)
    median.round
  }

  def exact_F0(x: RDD[String]): Long = x.distinct().count()

  def exact_F2(x: RDD[String]): Long = {
    x.map(s => (s, 1L)).reduceByKey(_ + _).map { case (_, c) => c * c }.reduce(_ + _)
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Project_2").getOrCreate()
    if (args.length < 2) {
      println("Usage: project_2 input_path option = {BJKST, tidemark, ToW, exactF2, exactF0} ")
      sys.exit(1)
    }
    val input_path = args(0)
    val df = spark.read.format("csv").load(input_path)
    val dfrdd = df.rdd.map(row => row.getString(0))
    val startTimeMillis = System.currentTimeMillis()

    val result = args(1) match {
      case "BJKST" if args.length == 4 =>
        val ans = BJKST(dfrdd, args(2).toInt, args(3).toInt)
        println(s"BJKST Estimate: $ans")
        ans
      case "tidemark" if args.length == 3 =>
        val ans = tidemark(dfrdd, args(2).toInt)
        println(s"Tidemark Estimate: $ans")
        ans
      case "ToW" if args.length == 4 =>
        val ans = Tug_of_War(dfrdd, args(2).toInt, args(3).toInt)
        println(s"Tug-of-War Estimate: $ans")
        ans
      case "exactF2" =>
        val ans = exact_F2(dfrdd)
        println(s"Exact F2: $ans")
        ans
      case "exactF0" =>
        val ans = exact_F0(dfrdd)
        println(s"Exact F0: $ans")
        ans
      case _ =>
        println("Invalid arguments.")
        sys.exit(1)
    }

    val durationSeconds = (System.currentTimeMillis() - startTimeMillis) / 1000
    println(s"Time elapsed: ${durationSeconds}s. Estimate: $result")
  }
}
