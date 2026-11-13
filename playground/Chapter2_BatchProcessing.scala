// =====================================================================================================
//  CHAPTER 2 - BATCH PROCESSING                                    study companion, runs in a browser
// =====================================================================================================
//  HOW TO USE
//    1. Open https://compilebytes.com/tools/scala , paste this whole file into Main.scala, press Run.
//    2. Read the code of a section, predict what it prints, then compare with the real output.
//    3. Too much output? Put section numbers in ONLY (just below), e.g. Set(11, 12), and run again.
//    4. Change things! Other data, other partition counts, add a persist() ... and see what happens.
//
//  WHAT IS THIS
//    The playground has no Spark library. So PART 2 (bottom half of this file) is "mini-Spark": a small
//    but honest re-implementation of the RDD API of the chapter. Same method names, same behaviour:
//    lazy transformations, partitions, lineage, shuffles, partitioners, persist, stages, tasks.
//    The code in PART 1 is therefore (almost) exactly what you would type in spark-shell.
//    PART 2 is only ~450 lines, so you can also READ how Spark does it.
//
//  EXTRAS that real Spark does not have (they make invisible things visible):
//      sc.measure("label") { ... }   prints how much work the cluster did inside the block
//      sc.logJobs = true             the driver prints every job / stage it schedules
//      sc.trace   = true             prints every partition that really gets computed
//      sc.crashWorker(n)             a worker dies: its cached partitions and shuffle files are gone
//      sc.showStages(rdd)            prints the stages Spark would build for an action on rdd
//      showPartitions(rdd)           prints which element sits in which partition (on which worker)
//    A line that starts with [worker-N] was printed by code running in a TASK on a worker,
//    not by the driver. Keep an eye on that prefix: "where does my code run?" is exam material.
// =====================================================================================================
import scala.collection.mutable
import scala.reflect.ClassTag

val ONLY: Set[Int] = Set()          // empty = run every section.   Example: Set(5, 9, 16)

@main def main(): Unit = {
  s01_generalPrinciple()            // batch processing, scale up vs scale out
  s02_mapReduce()                   // map - shuffle - reduce, word count
  s03_hadoop()                      // HDFS + MapReduce, the disk bottleneck
  s04_sparkAndRdds()                // RDD = distributed, resilient, immutable, lazy
  s05_firstExample()                // textFile - filter - count - take
  s06_creatingRdds()                // parallelize, textFile
  s07_transformations()             // map flatMap filter distinct / union intersection subtract cartesian
  s08_actions()                     // collect count take reduce foreach / takeSample takeOrdered saveAsTextFile
  s09_shortCircuit()                // why take(n) does not process everything
  s10_pairRdds()                    // key-value pairs, motivation
  s11_aggregatingPairs()            // groupByKey reduceByKey foldByKey aggregateByKey
  s12_joiningPairs()                // join leftOuterJoin rightOuterJoin
  s13_sortingAndOtherPairOps()      // sortByKey mapValues flatMapValues keys values
  s14_pairActions()                 // countByKey collectAsMap lookup
  s15_lineage()                     // the DAG, toDebugString
  s16_persisting()                  // persist cache unpersist, storage levels, iterative algorithms
  s17_partitioning()                // default partitioning, repartition, coalesce
  s18_pairPartitioning()            // partitioners: implicit, explicit, hash, range, custom
  s19_narrowAndWide()               // narrow / wide dependencies, pipelining
  s20_partitionerPreservation()     // which transformations keep the partitioner?
  s21_shuffling()                   // detecting shuffles, shuffle files, co-partitioning
  s22_minimizingShuffles()          // pre-partitioning, map-side combine, filter early, shuffle files vs persist
  s23_faultTolerance()              // recomputing lost partitions from lineage
  s24_execution()                   // driver, executors, jobs, stages, tasks
  s25_distributingActions()         // foldLeft vs fold vs aggregate
  s26_exercises()                   // the chapter's exercises on small data
  println("\nDone. Tip: set ONLY = Set(n) at the top to replay a single section.")
}

def section(n: Int, title: String)(body: => Unit): Unit =
  if (ONLY.isEmpty || ONLY.contains(n)) {
    println(); println("=" * 100); println(s"SECTION $n - $title"); println("=" * 100)
    body
  }
def sub(title: String): Unit = println(s"\n--- $title ---")

// =====================================================================================================
//  PART 1 - THE CHAPTER, SECTION BY SECTION
// =====================================================================================================

// -----------------------------------------------------------------------------------------------------
// 1. GENERAL PRINCIPLE
//    Batch processing: read a FINITE (bounded) dataset, process ALL of it, produce a finite output.
//    Data is collected over a period and processed offline: nightly ETL, reports, scientific analysis.
// -----------------------------------------------------------------------------------------------------
def s01_generalPrinciple(): Unit = section(1, "General principle: bounded input -> process it all -> bounded output") {
  // A nightly ETL job (Extract, Transform, Load) on ONE machine, with plain Scala collections.
  val rawOrders = List(                                                    // Extract: today's finite input
    "2025-08-23,alice,BE,120.50", "2025-08-23,bob,NL,80.00", "2025-08-23,carol,BE,15.25",
    "corrupt line", "2025-08-23,dave,FR,42.00", "2025-08-23,erin,NL,9.75")
  val orders = rawOrders.map(_.split(",")).filter(_.length == 4)           // Transform: parse + clean
  val revenuePerCountry = orders.groupMapReduce(_(2))(_(3).toDouble)(_ + _) // Transform: aggregate
  println("revenue per country = " + revenuePerCountry.toList.sorted)      // Load: write the report somewhere

  // This is fine while the data fits on one server. With terabytes (Volume) of unstructured weblogs
  // (Variety) the disk, CPU and memory of ONE machine become the bottleneck. You cannot keep buying a
  // bigger server (scale UP), so you spread the work over many machines (scale OUT):
  //   DISTRIBUTED batch processing = partition the data AND the computation, run the parts in parallel.
  val machines = rawOrders.grouped(2).toList                               // pretend: 3 machines, each holds a part
  machines.zipWithIndex.foreach { case (part, i) => println(s"   machine $i holds ${part.size} of the records") }
  println("   every machine can now parse/clean ITS records at the same time. Combining the per-country")
  println("   sums across machines is the hard part - that is what MapReduce (next section) solves.")
}

// -----------------------------------------------------------------------------------------------------
// 2. MAPREDUCE
//    Before MapReduce you had to program everything yourself: split the data, send work to nodes, collect
//    results, survive crashes. MapReduce (Google, 2004) hides all that behind TWO functions you write:
//       map    : applied to every input record, in parallel, emits (key, value) pairs
//       reduce : receives one key with ALL its values, combines them
//    In between, the framework does the SHUFFLE: bring all pairs with the same key to the same place.
// -----------------------------------------------------------------------------------------------------
def mapReduce[I, K, V, R](splits: Seq[Seq[I]])(map: I => Seq[(K, V)])(reduce: (K, Seq[V]) => R)(using Ordering[K]): Seq[(K, R)] = {
  // MAP phase: each machine works on its own local chunk, independently of the others
  val mapped: Seq[Seq[(K, V)]] = splits.map(chunk => chunk.flatMap(map))
  mapped.zipWithIndex.foreach { case (pairs, m) => println(s"   map     on machine $m -> ${pairs.mkString(" ")}") }
  // SHUFFLE phase (done by the framework, not by you): group all pairs by key, across ALL machines.
  // This moves data over the network -> the most expensive part of the whole job.
  val shuffled: Seq[(K, Seq[V])] = mapped.flatten.groupMap(_._1)(_._2).toSeq.sortBy(_._1)
  shuffled.foreach { case (k, vs) => println(s"   shuffle -> ($k, [${vs.mkString(", ")}])") }
  println(s"   (the shuffle moved ${mapped.map(_.size).sum} pairs over the network)")
  // REDUCE phase: each key, with the list of all its values, is reduced to one result
  val reduced = shuffled.map { case (k, vs) => (k, reduce(k, vs)) }
  reduced.foreach { case (k, r) => println(s"   reduce  -> ($k, $r)") }
  reduced
}

def s02_mapReduce(): Unit = section(2, "MapReduce: word count, the 'hello world' of Big Data") {
  // three machines, each stores one chunk of the documents
  val splits = Seq(Seq("Deer Bear River"), Seq("Car Car River"), Seq("Deer Car Bear"))
  val counts = mapReduce(splits)(line => line.split(" ").toSeq.map(word => (word, 1)))((word, ones) => ones.sum)
  println("final result = " + counts.mkString(", "))
  println("""
Why is the shuffle needed? Machine 0 AND machine 2 both produced (Deer,1). No single machine can know
the total for 'Deer' until all (Deer,1) pairs are in one place. Remember this picture: every time the
chapter says 'wide transformation' or 'shuffle', it is this step again.""")

  sub("the same engine solves other problems: only map and reduce change")
  val temperatures = Seq(Seq("2023 18", "2024 31"), Seq("2023 25", "2024 12"), Seq("2024 35", "2023 21"))
  val maxPerYear = mapReduce(temperatures)(line => { val p = line.split(" "); Seq((p(0), p(1).toInt)) })((year, temps) => temps.max)
  println("max temperature per year = " + maxPerYear.mkString(", "))
}

// -----------------------------------------------------------------------------------------------------
// 3. APACHE HADOOP
//    Open-source MapReduce + HDFS (a distributed file system that stores every block 3x).
//    Weak spot: Hadoop writes intermediate results to disk between steps. That gives extreme fault
//    tolerance, but every step of a multi-step job pays disk + network + replication, and the steps
//    cannot be pipelined. Worst for ITERATIVE algorithms (machine learning) and interactive queries.
// -----------------------------------------------------------------------------------------------------
def s03_hadoop(): Unit = section(3, "Apache Hadoop: the mandatory write-to-disk between steps") {
  object Hdfs {                                     // a fake HDFS that counts its I/O
    var recordsRead = 0L; var recordsWritten = 0L
    private val files = mutable.Map[String, Seq[Any]]()
    def write(path: String, data: Seq[Any], replication: Int = 3): Unit = { recordsWritten += data.size.toLong * replication; files(path) = data }
    def read[T](path: String): Seq[T] = { recordsRead += files(path).size; files(path).asInstanceOf[Seq[T]] }
    def reset(): Unit = { recordsRead = 0; recordsWritten = 0 }
    def io: String = s"records read from disk = $recordsRead, records written to disk (x3 replicas) = $recordsWritten"
  }
  Hdfs.write("/in/docs", Seq.fill(200)(Seq("Deer Bear River", "Car Car River", "Deer Car Bear")).flatten)   // 600 lines

  // A 3-step job: (1) count the words, (2) keep words that occur more than 250 times, (3) sort by count.
  sub("Hadoop style: every step is its own MapReduce job, reading from and writing to HDFS")
  Hdfs.reset()
  val step1 = Hdfs.read[String]("/in/docs").flatMap(_.split(" ")).groupMapReduce(identity)(_ => 1)(_ + _).toSeq
  Hdfs.write("/tmp/step1", step1)                                          // must be materialised on disk
  val step2 = Hdfs.read[(String, Int)]("/tmp/step1").filter(_._2 > 250)
  Hdfs.write("/tmp/step2", step2)                                          // again
  val step3 = Hdfs.read[(String, Int)]("/tmp/step2").sortBy(-_._2)
  Hdfs.write("/out/hadoop", step3)
  println(s"   result = $step3\n   ${Hdfs.io}")

  sub("Spark style: the steps are chained in memory, only the final result touches the disk")
  Hdfs.reset()
  val result = Hdfs.read[String]("/in/docs").flatMap(_.split(" ")).groupMapReduce(identity)(_ => 1)(_ + _).toSeq.filter(_._2 > 250).sortBy(-_._2)
  Hdfs.write("/out/spark", result)
  println(s"   result = $result\n   ${Hdfs.io}")

  sub("an iterative algorithm: 10 passes over the same data")
  Hdfs.reset(); for (_ <- 1 to 10) Hdfs.read[String]("/in/docs").size      // Hadoop: every iteration is a new job
  println(s"   Hadoop: ${Hdfs.io}")
  Hdfs.reset(); val inMemory = Hdfs.read[String]("/in/docs"); for (_ <- 1 to 10) inMemory.size   // Spark: read once, cache in RAM
  println(s"   Spark : ${Hdfs.io}   <- 'up to 100x faster for iterative algorithms'")
}

// -----------------------------------------------------------------------------------------------------
// 4. APACHE SPARK AND RDDs
//    Spark improves on Hadoop MapReduce with (1) in-memory computation, (2) one general-purpose engine
//    (batch, SQL, streaming, ML), (3) rich and concise APIs.
//    Core idea: data = immutable in-memory collections (RDDs), operations = functional transformations,
//    fault tolerance = remember the LINEAGE (how the data was made) instead of replicating the data.
//
//    RDD = Resilient Distributed Dataset. Key properties:
//      Distributed       : split into partitions that live on different workers -> parallelism
//      Resilient         : a lost partition is recomputed from its lineage (the "recipe")
//      Immutable         : never changed in place; a transformation creates a new RDD
//      Lazily evaluated  : transformations are only recorded in a DAG; an ACTION triggers computation
// -----------------------------------------------------------------------------------------------------
def s04_sparkAndRdds(): Unit = section(4, "Spark and RDDs: distributed, resilient, immutable, lazy") {
  // The SparkContext is the entry point for RDD programming. In real Spark it is part of the SparkSession:
  //   val spark = SparkSession.builder().appName("...").getOrCreate();  val sc = spark.sparkContext
  // and the `spark-shell` command gives you a REPL where `spark` and `sc` already exist.
  val sc = new SparkContext()

  sub("rich, concise API: the whole MapReduce word count of section 2 is one line")
  val docs = sc.parallelize(Seq("Deer Bear River", "Car Car River", "Deer Car Bear"), 3)
  val wordCounts = docs.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
  println("   " + wordCounts.collect().sorted.mkString(", "))

  sub("DISTRIBUTED: the data is split into partitions, spread over the workers")
  val numbers = sc.parallelize(1 to 12, 4)
  println(s"   numbers has ${numbers.getNumPartitions} partitions; a partition never spans machines, a machine can hold several")
  showPartitions(numbers)

  sub("IMMUTABLE: a transformation does not change the RDD, it returns a NEW one")
  val doubled = numbers.map(_ * 2)
  println("   numbers = " + numbers.collect().mkString(" "))
  println("   doubled = " + doubled.collect().mkString(" "))

  sub("LAZY: transformations only RECORD what must happen. An action makes it happen.")
  val bigOnes = numbers.map { n => println(s"map is looking at $n"); n * 10 }.filter(_ > 80)
  sc.measure("after map + filter, before any action") { bigOnes }
  println("   ^ no 'map is looking at' line was printed. Now the action collect():")
  val collected = sc.measure("collect") { bigOnes.collect() }
  println("   result = " + collected.mkString(" "))

  sub("RESILIENT: the RDD knows its lineage, the recipe to rebuild itself (section 15 and 23)")
  println(bigOnes.toDebugString)

  sub("operations come in two kinds - look at the TYPES")
  val evens: RDD[Int] = numbers.filter(_ % 2 == 0)      // transformation: lazy, returns an RDD
  val howMany: Long   = evens.count()                   // action: runs a job, returns a plain value
  println(s"   filter returned an RDD (nothing ran); count() returned the value $howMany")
}

// -----------------------------------------------------------------------------------------------------
// 5. A FIRST EXAMPLE (the log file example of the chapter, verbatim)
// -----------------------------------------------------------------------------------------------------
def s05_firstExample(): Unit = section(5, "A first example: textFile -> filter -> count -> take") {
  val sc = new SparkContext()
  // create "example.log" in the fake file system: 20 000 lines, every 10th line is an ERROR
  sc.writeFile("example.log", (1 to 20000).map(i => if (i % 10 == 0) s"2025-08-23 ERROR request $i failed" else s"2025-08-23 INFO request $i ok"))
  sc.blockSize = 5000                                    // 5000 lines per block -> the file has 4 blocks -> 4 partitions

  // load data into an RDD
  val logLines: RDD[String] = sc.textFile("example.log")
  // transform the data to find error lines
  val errorLines: RDD[String] = logLines.filter(line => line.contains("ERROR"))
  sc.measure("textFile + filter (both lazy: the file has not even been opened)") { errorLines }

  // perform an action to get the result
  val numErrors: Long = sc.measure("count()") { errorLines.count() }
  println(s"The log file contains ${numErrors} error lines.")

  // example of another action to inspect the data
  println("First 5 error lines:")
  sc.measure("take(5)") { errorLines.take(5) }.foreach(println)
  println("   ^ take(5) stopped as soon as it had 5 lines: 50 lines read instead of 20000 (section 9)")

  sub("laziness, proven: a file that does not exist only fails when an ACTION runs")
  val ghost = sc.textFile("does-not-exist.log").filter(_.contains("ERROR"))   // no error here...
  try ghost.count() catch { case e: Exception => println(s"   count() failed: ${e.getMessage}") }
}

// -----------------------------------------------------------------------------------------------------
// 6. CREATING RDDs
//    (a) parallelize: turn a collection that lives in the driver into an RDD (prototyping, tests)
//    (b) textFile   : point to data in external storage (local fs, HDFS, S3, ...) - the real-world way
// -----------------------------------------------------------------------------------------------------
def s06_creatingRdds(): Unit = section(6, "Creating RDDs: parallelize and textFile") {
  val sc = new SparkContext()

  sub("def parallelize[T](seq: Seq[T], numPartitions: Int = defaultParallelism): RDD[T]")
  // create an RDD of integers from a local Scala List
  val data = List(1, 2, 3, 4, 5)
  val rdd = sc.parallelize(data)
  println(s"   default: ${rdd.getNumPartitions} partitions (= total number of cores). Spark runs one task per partition.")
  showPartitions(rdd)
  val rdd2 = sc.parallelize(data, 2)                     // set the number of partitions yourself
  println("   with numPartitions = 2 (the elements keep the order of the original collection):")
  showPartitions(rdd2)
  println("   rule of thumb from the Spark docs: 2 to 4 partitions per CPU in the cluster")

  sub("def textFile(path: String, minPartitions: Int = defaultMinPartitions): RDD[String]")
  sc.writeFile("poem.txt", Seq("roses are red", "violets are blue", "spark is lazy", "and so are you", "the end"))
  sc.blockSize = 2                                       // tiny blocks so that you can see them: 2 lines per block
  val lines: RDD[String] = sc.textFile("poem.txt")       // each element = one line of the file
  println(s"   ${lines.count()} lines in ${lines.getNumPartitions} partitions (one partition per block of the file)")
  showPartitions(lines)
  println("   on a local file system the file must exist on ALL nodes. Other sources: HDFS, Cassandra, S3, ...")
}

// -----------------------------------------------------------------------------------------------------
// 7. TRANSFORMATIONS: lazy operations that create a new RDD from an existing one.
//    You recognise them by their return type: RDD. They only extend the lineage graph (a DAG).
// -----------------------------------------------------------------------------------------------------
def s07_transformations(): Unit = section(7, "Transformations (single RDD and set operations)") {
  val sc = new SparkContext()
  def show[T](label: String, rdd: RDD[T]): Unit = println(f"   $label%-46s -> ${sc.runJob(rdd, "show")(_.toVector).flatten.mkString(", ")}")

  sub("transformations on a single RDD")
  val lines = sc.parallelize(Seq("to be or", "not to be"), 2)
  val nums  = sc.parallelize(Seq(1, 2, 3, 4, 4, 2), 2)
  show("map(_.length)          one in, one out", lines.map(_.length))                  // def map[B](f: A => B): RDD[B]
  show("map(_.split(\" \"))      (arrays!)", lines.map(_.split(" ").mkString("[", "|", "]")))
  show("flatMap(_.split(\" \"))  one in, 0..n out", lines.flatMap(_.split(" ")))       // def flatMap[B](f: A => IterableOnce[B]): RDD[B]
  show("filter(_ % 2 == 0)     keep what matches", nums.filter(_ % 2 == 0))            // def filter(f: A => Boolean): RDD[A]
  show("distinct()             duplicates removed", nums.distinct())                   // def distinct(): RDD[A]

  sub("set transformations on two RDDs")
  val a = sc.parallelize(Seq(1, 2, 3, 3), 2)
  val b = sc.parallelize(Seq(3, 4, 5), 2)
  show("a.union(b)             duplicates stay!", a.union(b))                          // like list concatenation
  show("a.intersection(b)      result is unique", a.intersection(b))
  show("a.subtract(b)          in a, not in b", a.subtract(b))
  show("a.cartesian(b)         every pair (a, b)", a.cartesian(b))
  println(s"   cartesian: ${a.count()} x ${b.count()} = ${a.cartesian(b).count()} pairs. With 1M x 1M records that is 10^12 pairs: VERY expensive.")

  sub("all of these returned RDDs: nothing ran until the actions inside show(...) were called")
  val pipeline = nums.map(_ + 1).filter(_ > 2).distinct()
  sc.measure("building a pipeline of 3 transformations") { pipeline }
}

// -----------------------------------------------------------------------------------------------------
// 8. ACTIONS: operations that trigger a job. They return a value to the driver (or write to storage).
//    The lineage (logical plan) is turned into a physical job that runs on the cluster.
// -----------------------------------------------------------------------------------------------------
def s08_actions(): Unit = section(8, "Actions") {
  val sc = new SparkContext()
  val nums = sc.parallelize(Seq(5, 3, 8, 1, 9, 2, 7), 3)

  sub("typical actions")
  val all: Array[Int] = nums.collect()                   // def collect(): Array[A]   ALL elements go to the driver
  println("   collect()       = " + all.mkString(", ") + "     (careful: OutOfMemoryError in the driver if the RDD is big)")
  println("   count()         = " + nums.count())        // def count(): Long
  println("   take(3)         = " + nums.take(3).mkString(", ") + "               (first 3; only use when the result is small)")
  println("   reduce(_ + _)   = " + nums.reduce(_ + _))  // def reduce(f: (A, A) => A): A

  sub("reduce needs a COMMUTATIVE and ASSOCIATIVE function: partitions are reduced separately, then combined")
  val oneToEight = (1 to 8)
  println("   1 partition : (1 to 8).reduce(_ - _) = " + sc.parallelize(oneToEight, 1).reduce(_ - _))
  println("   4 partitions: (1 to 8).reduce(_ - _) = " + sc.parallelize(oneToEight, 4).reduce(_ - _) + "   <- different answer! subtraction is neither")
  println("   + is both, so the answer never depends on the partitioning: " + sc.parallelize(oneToEight, 1).reduce(_ + _) + " = " + sc.parallelize(oneToEight, 4).reduce(_ + _))

  sub("foreach(f): f runs on the WORKERS (side effects: printing, writing to a database)")
  nums.foreach(n => println(s"saw $n"))                  // def foreach(f: A => Unit): Unit
  println("   ^ on a real cluster these lines end up in the executor logs, NOT on your screen (section 24)")

  sub("other useful actions")
  println("   takeSample(false, 3) = " + nums.takeSample(withReplacement = false, 3).mkString(", ") + "    (each element at most once)")
  println("   takeSample(true, 10) = " + nums.takeSample(withReplacement = true, 10).mkString(", ") + "    ('putting back': repeats possible)")
  println("   takeOrdered(3)       = " + nums.takeOrdered(3).mkString(", ") + "    (natural order)")
  println("   takeOrdered(3)(desc) = " + nums.takeOrdered(3)(Ordering[Int].reverse).mkString(", ") + "    (custom comparator)")
  nums.map(_ * 100).saveAsTextFile("out/numbers")        // def saveAsTextFile(path: String): Unit
  println("   saveAsTextFile(\"out/numbers\") wrote a DIRECTORY with one part-file per partition:")
  sc.fs.keys.filter(_.startsWith("out/numbers")).foreach(f => println(s"      $f   ${sc.fs(f).mkString("[", ", ", "]")}"))
}

// -----------------------------------------------------------------------------------------------------
// 9. SHORT-CIRCUIT EVALUATION
//    someRdd.filter(...).map(...).take(n): because nothing ran yet, Spark knows at job start that it only
//    needs n records. It processes just enough partitions/elements and stops. The benefit applies to the
//    whole chain of lazy transformations before the action (like lazy lists / streams in Scala, Haskell).
// -----------------------------------------------------------------------------------------------------
def s09_shortCircuit(): Unit = section(9, "Short-circuit evaluation: take(n) stops early") {
  val sc = new SparkContext()
  var filterCalls = 0      // (counting through a captured variable works here because this simulation is one JVM;
                           //  on a real cluster every worker gets its own copy - use an accumulator there)
  val million = 1 to 1000000

  sub("lazy RDD pipeline")
  val firstThree = sc.parallelize(million, 4).filter { n => filterCalls += 1; n % 7 == 0 }.map(n => n * n).take(3)
  println(s"   result = ${firstThree.mkString(", ")}   filter was called $filterCalls times")

  sub("eager Scala collection: the whole list is filtered before take can start")
  filterCalls = 0
  val eager = million.toList.filter { n => filterCalls += 1; n % 7 == 0 }.map(n => n.toLong * n).take(3)
  println(s"   result = ${eager.mkString(", ")}   filter was called $filterCalls times")

  sub("the same laziness exists in plain Scala: iterators / LazyList")
  filterCalls = 0
  val lazily = million.iterator.filter { n => filterCalls += 1; n % 7 == 0 }.map(n => n * n).take(3).toList
  println(s"   result = ${lazily.mkString(", ")}   filter was called $filterCalls times")
}

// -----------------------------------------------------------------------------------------------------
// 10. PAIR RDDs: RDDs whose elements are key-value pairs, RDD[(K, V)].
//     Spark has special, efficient operations for them: aggregating, joining, sorting, partitioning by key.
//     Motivation: project big nested records down to (key, value) so you can group / aggregate per key.
// -----------------------------------------------------------------------------------------------------
case class User(userId: String, name: String, country: String)
case class Session(sessionId: String, durationSeconds: Int)
case class LogEvent(eventId: String, timestamp: String, user: User, session: Session)   // the JSON record of the chapter

def s10_pairRdds(): Unit = section(10, "Pair RDDs: motivation") {
  val sc = new SparkContext()
  val events: RDD[LogEvent] = sc.parallelize(Seq(
    LogEvent("evt_12345", "2025-08-23T15:10:00Z", User("u_925903468", "Alice", "BE"), Session("sess_xyz", 120)),
    LogEvent("evt_12346", "2025-08-23T15:12:00Z", User("u_111111111", "Bob", "NL"), Session("sess_abc", 300)),
    LogEvent("evt_12347", "2025-08-23T16:00:00Z", User("u_925903468", "Alice", "BE"), Session("sess_def", 45)),
    LogEvent("evt_12348", "2025-08-23T17:30:00Z", User("u_111111111", "Bob", "NL"), Session("sess_ghi", 15))))

  // "total time spent per user": project every complex record down to (user_id, duration_seconds) ...
  val pairs: RDD[(String, Int)] = events.map(e => (e.user.userId, e.session.durationSeconds))
  println("   projected to pairs    : " + pairs.collect().mkString(", "))
  // ... the key groups all sessions of one user, the values are aggregated (here: summed)
  val totalPerUser: RDD[(String, Int)] = pairs.reduceByKey(_ + _)
  println("   total seconds per user: " + totalPerUser.collect().mkString(", "))

  sub("a Pair RDD is NOT a Map: duplicate keys are perfectly normal")
  println("   pairs has " + pairs.count() + " elements but only " + pairs.keys.distinct().count() + " distinct keys")
  println("   a Pair RDD can have a partitioner (pairs with the same key in the same partition). Right now: " + pairs.partitioner)
  println("   after reduceByKey: " + totalPerUser.partitioner + "   (set implicitly, see section 18)")
}

// -----------------------------------------------------------------------------------------------------
// 11. AGGREGATING PAIRS: many pairs with duplicate keys -> one pair per (now unique) key
// -----------------------------------------------------------------------------------------------------
// (ProductCategory, SaleAmount) - used by several sections
val salesData = List(
  ("Electronics", 500),
  ("Books", 20),
  ("Electronics", 1300),
  ("Groceries", 45),
  ("Books", 30),
  ("Electronics", 300)
)

def s11_aggregatingPairs(): Unit = section(11, "Aggregating pairs: groupByKey, reduceByKey, foldByKey, aggregateByKey") {
  val sc = new SparkContext()
  val salesRdd: RDD[(String, Int)] = sc.parallelize(salesData)

  sub("def groupByKey(): RDD[(K, Iterable[V])]")
  val groupedSalesRdd: RDD[(String, Iterable[Int])] = salesRdd.groupByKey()
  groupedSalesRdd.collect().foreach(println)
  println("   careful: ALL values of one key must fit in the memory of ONE worker (OutOfMemoryError for hot keys)")
  println("   (real Spark prints CompactBuffer(...) where the book writes Seq(...); the order of the keys is not guaranteed)")

  sub("def reduceByKey(func: (V, V) => V): RDD[(K, V)]")
  val totalSalesRdd: RDD[(String, Int)] = salesRdd.reduceByKey(_ + _)
  totalSalesRdd.collect().foreach(println)

  sub("def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]      like reduceByKey, with a start value")
  salesRdd.foldByKey(0)(_ + _).collect().foreach(println)
  println("   max per key with foldByKey(Int.MinValue)(math.max): " + salesRdd.foldByKey(Int.MinValue)(math.max).collect().mkString(", "))

  sub("def aggregateByKey[U](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U): RDD[(K, U)]")
  // the result type U may DIFFER from the value type V. Here V = Int and U = (sum, count).
  val seqOp = (acc: (Int, Int), value: Int) => (acc._1 + value, acc._2 + 1)                     // merge a V into a U (inside one partition)
  val combOp = (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)   // merge two U's (from different partitions)
  val salesSummaryRdd: RDD[(String, (Int, Int))] = salesRdd.aggregateByKey((0, 0))(seqOp, combOp)
  salesSummaryRdd.collect().foreach(println)

  sub("average = sum / count, which is why the intermediate state had to be a pair")
  val averageSalesRdd = salesSummaryRdd.mapValues { case (sum, count) => sum.toDouble / count }
  averageSalesRdd.collect().foreach(println)
  println("   (an average cannot be done with reduceByKey directly: the average of averages is wrong)")
}

// -----------------------------------------------------------------------------------------------------
// 12. JOINING PAIRS: combine two Pair RDDs on their common keys (enrich one dataset with another)
// -----------------------------------------------------------------------------------------------------
def s12_joiningPairs(): Unit = section(12, "Joining pairs: join, leftOuterJoin, rightOuterJoin") {
  val sc = new SparkContext()
  val managerData = List(
    ("Electronics", "Alice"),
    ("Books", "Bob"),
    ("Apparel", "Charlie")
  )
  val categoryManagersRdd: RDD[(String, String)] = sc.parallelize(managerData)
  val totalSalesRdd: RDD[(String, Int)] = sc.parallelize(salesData).reduceByKey(_ + _)
  println("   left  (totalSales)      : " + totalSalesRdd.collect().mkString(", "))
  println("   right (categoryManagers): " + categoryManagersRdd.collect().mkString(", "))

  sub("def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))]                    inner join: keys present in BOTH")
  val innerJoinRdd: RDD[(String, (Int, String))] = totalSalesRdd.join(categoryManagersRdd)
  innerJoinRdd.collect().foreach(println)

  sub("def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))]   ALL keys of the left RDD")
  val leftOuterJoinRdd: RDD[(String, (Int, Option[String]))] = totalSalesRdd.leftOuterJoin(categoryManagersRdd)
  leftOuterJoinRdd.collect().foreach(println)

  sub("def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))]  ALL keys of the right RDD")
  val rightOuterJoinRdd: RDD[(String, (Option[Int], String))] = totalSalesRdd.rightOuterJoin(categoryManagersRdd)
  rightOuterJoinRdd.collect().foreach(println)

  sub("extra: fullOuterJoin keeps the keys of both sides (used later for streaming joins)")
  totalSalesRdd.fullOuterJoin(categoryManagersRdd).collect().foreach(println)

  sub("a join multiplies when keys are duplicated: 2 pairs left x 2 pairs right = 4 results for that key")
  val l = sc.parallelize(Seq(("k", 1), ("k", 2)))
  val r = sc.parallelize(Seq(("k", "a"), ("k", "b")))
  println("   " + l.join(r).collect().mkString(", "))
}

// -----------------------------------------------------------------------------------------------------
// 13. SORTING AND OTHER PAIR TRANSFORMATIONS
// -----------------------------------------------------------------------------------------------------
def s13_sortingAndOtherPairOps(): Unit = section(13, "sortByKey, mapValues, flatMapValues, keys, values") {
  val sc = new SparkContext()
  val totalSalesRdd: RDD[(String, Int)] = sc.parallelize(salesData).reduceByKey(_ + _)

  sub("def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length): RDD[(K, V)]")
  val sortedSalesRdd: RDD[(String, Int)] = totalSalesRdd.sortByKey()
  sortedSalesRdd.collect().foreach(println)
  println("   descending: " + totalSalesRdd.sortByKey(ascending = false).collect().mkString(", "))

  sub("def mapValues[U](f: V => U): RDD[(K, U)]        the key is untouched -> the partitioner is preserved")
  val formattedSalesRdd: RDD[(String, String)] = sortedSalesRdd.mapValues(amount => s"EUR ${amount}.00")
  formattedSalesRdd.collect().foreach(println)
  println("   rdd.mapValues(f) gives the same DATA as rdd.map { case (k, v) => (k, f(v)) }, but map loses the partitioner (section 20)")

  sub("def flatMapValues[U](f: V => IterableOnce[U]): RDD[(K, U)]     0..n new values, each paired with the original key")
  val detailedRecordsRdd: RDD[(String, (String, Double))] = formattedSalesRdd.flatMapValues { amountString =>
    val amount = amountString.stripPrefix("EUR").stripSuffix(".00").toDouble
    List(("Sales", amount), ("Tax", amount * 0.1))
  }
  detailedRecordsRdd.collect().foreach(println)

  sub("def keys: RDD[K]     def values: RDD[V]     (transformations! the result could be far too big for the driver)")
  val categoriesRdd: RDD[String] = detailedRecordsRdd.keys
  categoriesRdd.distinct().collect().foreach(println)
  val amountsRdd: RDD[(String, Double)] = detailedRecordsRdd.values
  amountsRdd.collect().foreach(println)
}

// -----------------------------------------------------------------------------------------------------
// 14. PAIR ACTIONS: return a result to the driver
// -----------------------------------------------------------------------------------------------------
def s14_pairActions(): Unit = section(14, "Pair actions: countByKey, collectAsMap, lookup") {
  val sc = new SparkContext()
  val salesRdd: RDD[(String, Int)] = sc.parallelize(salesData)

  val counts: Map[String, Long] = salesRdd.countByKey()              // def countByKey(): Map[K, Long]
  println("   countByKey()            = " + counts)
  val asMap: Map[String, Int] = salesRdd.collectAsMap()              // def collectAsMap(): Map[K, V]
  println("   collectAsMap()          = " + asMap + "   <- 6 pairs became 3: a Map keeps ONE value per key")
  println("   reduce first, then it is safe: " + salesRdd.reduceByKey(_ + _).collectAsMap())
  val electronics: Seq[Int] = salesRdd.lookup("Electronics")         // def lookup(key: K): Seq[V]
  println("   lookup(\"Electronics\")   = " + electronics)

  sub("lookup is smarter on a partitioned RDD: only the ONE partition that can hold the key is scanned")
  sc.measure("lookup without partitioner") { salesRdd.lookup("Books") }
  val partitioned = salesRdd.partitionBy(new HashPartitioner(4)).persist()
  partitioned.count()
  sc.measure("lookup with partitioner   ") { partitioned.lookup("Books") }
}

// -----------------------------------------------------------------------------------------------------
// 15. LINEAGE
//     The lineage graph is a DAG that contains ONLY RDDs and the transformations between them (no
//     actions). It is the logical recipe to compute an RDD from its sources. It is what makes laziness,
//     fault tolerance and execution planning possible.
// -----------------------------------------------------------------------------------------------------
def s15_lineage(): Unit = section(15, "Lineage: toDebugString") {
  sub("a linear lineage")
  val sc = new SparkContext()
  val data = Seq(
    "Apache Spark is fast",
    "Spark is powerful",
    "RDD lineage is a DAG"
  )
  val rdd0 = sc.parallelize(data)
  val rdd1 = rdd0.flatMap(_.split(" "))
  val rdd2 = rdd1.filter(_.length > 3)
  val rdd3 = rdd2.map(_.toLowerCase)
  println("\n=== RDD Lineage ===")
  println(rdd3.toDebugString)
  println("   read it bottom-up: parallelize -> flatMap -> filter -> map. (4) = number of partitions.")
  sc.measure("building rdd0..rdd3 and printing the lineage") { rdd3 }

  sub("a non-linear lineage: fork and merge")
  val sc2 = new SparkContext()
  val base = sc2.parallelize(Seq(1, 2, 3, 4, 5))
  val even = base.filter(_ % 2 == 0)   // branch 1
  val odd  = base.filter(_ % 2 == 1)   // branch 2
  val combined = even.union(odd)
  println("\n=== RDD Lineage ===")
  println(combined.toDebugString)
  println("   only FOUR RDDs (ids 0..3): RDD[0] appears twice because both branches start from it.")

  sub("a lineage with a shuffle: the indented '+-' block is a different stage (section 21 and 24)")
  val sc3 = new SparkContext()
  val counts = sc3.parallelize(data).flatMap(_.split(" ")).map(w => (w, 1)).reduceByKey(_ + _).filter(_._2 > 1)
  println(counts.toDebugString)
}

// -----------------------------------------------------------------------------------------------------
// 16. PERSISTING
//     Lazy evaluation has a price: every action recomputes the RDD from scratch, the whole lineage.
//     If you use an RDD more than once, persist() it: computed once, then kept (memory/disk).
// -----------------------------------------------------------------------------------------------------
case class Point(x: Vector[Double], y: Double)

def s16_persisting(): Unit = section(16, "Persisting: persist, cache, unpersist, storage levels") {
  val sc = new SparkContext()
  sc.writeFile("huge-server.log", (1 to 9000).map(i => if (i % 30 == 0) s"FATAL ERROR $i" else if (i % 3 == 0) s"ERROR $i" else s"INFO $i"))
  sc.blockSize = 3000
  val logs = sc.textFile("huge-server.log")

  sub("WITHOUT persist: both actions re-read and re-filter the whole file")
  val errorsNoPersist = logs.filter(_.contains("ERROR"))
  sc.measure("action 1: count  ") { errorsNoPersist.count() }
  sc.measure("action 2: collect") { errorsNoPersist.filter(_.contains("FATAL")).collect() }

  sub("WITH persist (the example of the chapter)")
  val errors = logs.filter(_.contains("ERROR")).persist()
  sc.measure("persist() itself is lazy") { errors }
  // Action 1: computes 'errors' and stores it in the cache
  val errorCount = sc.measure("action 1: count  ") { errors.count() }
  // Action 2: reuses the 'errors' RDD from the cache
  val fatalErrors = sc.measure("action 2: collect") { errors.filter(_.contains("FATAL")).collect() }
  println(s"   $errorCount errors, ${fatalErrors.length} of them fatal")
  println(errors.toDebugString)

  sub("the flip side: persist materialises WHOLE partitions, so take(2) loses its short-circuit")
  val a = logs.filter(_.contains("ERROR"));           sc.measure("take(2) without persist") { a.take(2) }
  val b = logs.filter(_.contains("ERROR")).persist(); sc.measure("take(2) with persist   ") { b.take(2) }

  sub("iterative algorithms: logistic regression (the maths is not important, the execution model is)")
  val rnd = new scala.util.Random(42)
  sc.writeFile("points.txt", (1 to 600).map { _ => val (x1, x2) = (rnd.nextGaussian(), rnd.nextGaussian()); s"$x1 $x2 ${if (x1 + x2 > 0) 1 else -1}" })
  def parsePoint(line: String): Point = { val p = line.split(" ").map(_.toDouble); Point(p.init.toVector, p.last) }
  def dot(a: Vector[Double], b: Vector[Double]): Double = a.zip(b).map { case (u, v) => u * v }.sum
  def train(points: RDD[Point], numIterations: Int = 10, alpha: Double = 0.01): Vector[Double] = {
    var w = Vector(0.0, 0.0)
    for (i <- 1 to numIterations) {
      val gradient = points.map { p =>                                   // a transformation ...
        val factor = (1 / (1 + math.exp(-p.y * dot(w, p.x))) - 1) * p.y
        p.x.map(_ * factor)
      }.reduce((g1, g2) => g1.zip(g2).map { case (u, v) => u + v })      // ... and an ACTION, inside the loop!
      w = w.zip(gradient).map { case (wi, gi) => wi - alpha * gi }
    }
    w
  }
  val wSlow = sc.measure("10 iterations, points NOT persisted") { train(sc.textFile("points.txt").map(parsePoint)) }
  val wFast = sc.measure("10 iterations, points persisted    ") { train(sc.textFile("points.txt").map(parsePoint).persist()) }
  def rounded(w: Vector[Double]): String = w.map(x => math.round(x * 1000) / 1000.0).mkString("(", ", ", ")")
  println(s"   same weights either way: ${rounded(wSlow)} = ${rounded(wFast)}")
  println("   without persist the file is read and parsed again in EVERY iteration (10 x 600 lines).")
  println("   persist before a loop = hoisting a loop-invariant computation out of the loop.")

  sub("def persist(newLevel: StorageLevel): RDD[T]      the storage levels")
  StorageLevel.all.foreach(level => println(f"   ${level.name}%-22s ${level.description}"))
  println("   cache() is exactly persist(StorageLevel.MEMORY_ONLY): " + (sc.parallelize(Seq(1)).cache().storageLevel == StorageLevel.MEMORY_ONLY))
  val onDisk = sc.parallelize(1 to 10).persist(StorageLevel.MEMORY_AND_DISK_2)
  println("   persist(StorageLevel.MEMORY_AND_DISK_2) -> " + onDisk.storageLevel.description)

  sub("def unpersist(blocking: Boolean = false): RDD[T]   free the memory when the data is no longer needed")
  // an RDD that is constant across all iterations
  val staticData = sc.parallelize((1 to 100).map(i => (i % 10, i))).cache()
  for (i <- 1 to 3) {
    // intermediate data specific to this iteration is joined and cached
    val intermediateData = sc.parallelize(Seq((i, s"iteration-$i"))).join(staticData).cache()
    // ... perform transformations and actions using intermediateData ...
    val result = intermediateData.count() + intermediateData.values.map(_._2).reduce(_ + _)
    println(s"   Iteration $i result: $result   cached blocks before unpersist: ${sc.cachedBlocks}")
    // manually release the memory used by this iteration's data
    intermediateData.unpersist()
  }
  println(s"   cached blocks at the end: ${sc.cachedBlocks} (only staticData). Without unpersist the cache keeps growing")
  println("   until Spark evicts the Least Recently Used partitions by itself.")
}

// -----------------------------------------------------------------------------------------------------
// 17. PARTITIONING
//     A partition lives on one worker and is the unit of parallelism: one task per partition.
//     Why care? (1) performance tuning: number of partitions = level of parallelism, balance;
//               (2) shuffle optimisation: organise Pair RDDs by key to AVOID shuffles (next section).
// -----------------------------------------------------------------------------------------------------
def s17_partitioning(): Unit = section(17, "Partitioning: defaults, repartition, coalesce") {
  val sc = new SparkContext()

  sub("default partitioning depends on the source")
  sc.writeFile("blocks.txt", (1 to 10).map(i => s"line$i")); sc.blockSize = 4
  println("   textFile: one partition per block of the file (data locality: process the block where it is stored)")
  showPartitions(sc.textFile("blocks.txt"))
  println("   parallelize: numPartitions slices (default = number of cores), keeping the order of the collection")
  val rdd = sc.parallelize(1 to 20, 4)
  showPartitions(rdd)

  sub("def repartition(numPartitions: Int): RDD[T]      more OR fewer partitions, always a FULL shuffle, balanced result")
  val more = rdd.repartition(8)
  sc.measure("repartition(8)") { more.count() }
  showPartitions(more)

  sub("def coalesce(numPartitions: Int, shuffle: Boolean = false): RDD[T]     fewer partitions WITHOUT shuffle: neighbours are merged")
  val fewer = rdd.coalesce(2)
  sc.measure("coalesce(2)") { fewer.count() }
  showPartitions(fewer)
  println("   coalesce(8) without shuffle cannot split partitions: still " + rdd.coalesce(8).getNumPartitions + " partitions")
  sc.measure("coalesce(2, shuffle = true) == repartition(2)") { rdd.coalesce(2, shuffle = true).count() }

  sub("typical use: coalesce after a filter that removed most of the data")
  val rare = sc.parallelize(1 to 100000, 8).filter(_ % 25000 == 0)
  println("   after the filter: 8 partitions with sizes " + sc.runJob(rare, "sizes")(_.size).mkString(", ") + "  -> 8 tasks for 4 records")
  println("   after coalesce(1): sizes " + sc.runJob(rare.coalesce(1), "sizes")(_.size).mkString(", "))
  println("   neither repartition nor coalesce looks at the CONTENT of the records. For that you need Pair RDDs.")
}

// -----------------------------------------------------------------------------------------------------
// 18. PAIR PARTITIONING
//     Content-aware partitioning only exists for Pair RDDs. A Partitioner is the rule key -> partition id.
//     It is set implicitly (by operations that need co-located keys) or explicitly (partitionBy).
//     Two built-in schemes: hash partitioning and range partitioning. You can also write your own.
// -----------------------------------------------------------------------------------------------------
// (ProductCategory, SaleAmount)
val salesData10 = List(
  ("Electronics", 500),
  ("Books", 20),
  ("Zen-Decor", 385),
  ("Electronics", 1300),
  ("Groceries", 45),
  ("Books", 30),
  ("Electronics", 300),
  ("Groceries", 150),
  ("Apparel", 100),
  ("Books", 70)
)

// A custom partitioner (= exercise 5): all domains with the same Top-Level Domain go to the same partition.
class TldPartitioner(numParts: Int) extends Partitioner {
  // the total number of partitions to create
  override def numPartitions: Int = numParts
  // returns the partition ID (from 0 to numPartitions-1) for any given key
  override def getPartition(key: Any): Int = {
    val domain = key.toString
    val tld = domain.substring(domain.lastIndexOf('.') + 1)          // "research.vub.be" -> "be"
    val mod = tld.hashCode % numPartitions
    if (mod < 0) mod + numPartitions else mod
  }
  // returns true if other is identical to this partitioner (Spark uses this to detect co-partitioning!)
  override def equals(other: Any): Boolean = other match {
    case t: TldPartitioner => t.numPartitions == numPartitions
    case _                 => false
  }
  override def hashCode: Int = numPartitions
}

def s18_pairPartitioning(): Unit = section(18, "Pair partitioning: implicit, explicit, hash, range, custom") {
  val sc = new SparkContext()
  val numPartitions = 4
  val salesRdd: RDD[(String, Int)] = sc.parallelize(salesData10, numPartitions)

  sub("IMPLICIT partitioning: a by-product of operations that need all pairs of a key together")
  println(s"Original RDD Partitioner: ${salesRdd.partitioner}")
  val defaultPartitionedContents = salesRdd.mapPartitionsWithIndex {
    (partitionId, iterator) =>
      iterator.map { element =>
        (partitionId, element) }
  }
  println(s"Key-unaware distribution of keys across $numPartitions partitions:")
  defaultPartitionedContents.collect().foreach(println)
  println("   ^ just the order of the source list: 'Electronics' sits in partitions 0, 1 and 2")

  val groupedRdd: RDD[(String, Iterable[Int])] = salesRdd.groupByKey()
  println(s"Grouped RDD Partitioner: ${groupedRdd.partitioner}")
  val partitionedContents = groupedRdd.mapPartitionsWithIndex {
    (partitionId, iterator) =>
      iterator.map { case (key, value) =>
        (partitionId, key, key.hashCode() % numPartitions, value) }.toList.iterator
  }
  println(s"Key-aware distribution of keys across $numPartitions partitions:")
  partitionedContents.collect().foreach(println)
  println("   ^ every key now lives in exactly one partition. No pairs ended up in partition 0.")

  sub("HASH partitioning: new HashPartitioner(numPartitions)")
  //   val mod = key.hashCode() % numPartitions
  //   val partition = mod + (if (mod < 0) numPartitions else 0)
  val hashPartitioner = new HashPartitioner(numPartitions)
  salesData10.map(_._1).distinct.foreach { key =>
    val mod = key.hashCode() % numPartitions
    println(f"   $key%-12s hashCode = ${key.hashCode()}%11d   hashCode %% 4 = $mod%2d   -> partition ${hashPartitioner.getPartition(key)}")
  }
  println("   a negative modulo gets numPartitions added (-1 becomes 3). Same key -> same partition, always.")
  println("   BUT no ordering at all: which keys share a partition is arbitrary.")

  sub("EXPLICIT partitioning: def partitionBy(partitioner: Partitioner): RDD[(K, V)]")
  val partitionedRdd = salesRdd.partitionBy(new HashPartitioner(2)).persist()     // persist! otherwise the shuffle is redone
  println(s"   partitioner after partitionBy: ${partitionedRdd.partitioner}")
  showPartitions(partitionedRdd)
  println("   you do this not for the result itself but to PREPARE the layout: later key-based operations")
  println("   on this RDD (joins, reduceByKey, ...) can then run without shuffling (sections 21 and 22).")
  // "all strings that start with the same letter in the same partition": impossible for a plain RDD[String],
  // easy once the first letter is the KEY of a Pair RDD
  val words = sc.parallelize(Seq("apple", "banana", "avocado", "blueberry", "cherry", "apricot", "cranberry"), 3)
  println("   plain RDD[String].partitioner = " + words.partitioner + "  -> make the first letter the key, then partitionBy:")
  showPartitions(words.map(w => (w.head, w)).partitionBy(new HashPartitioner(3)))

  sub("RANGE partitioning: new RangePartitioner(numPartitions, rdd, ascending = true)   - done implicitly by sortByKey")
  val sc2 = new SparkContext()
  val salesRdd2: RDD[(String, Int)] = sc2.parallelize(salesData10)
  val numPartitions2 = 2
  // implicit range partitioning
  val sortedSalesRdd: RDD[(String, Int)] = salesRdd2.sortByKey(ascending = true, numPartitions2)
  println(s"sorted sales RDD Partitioner: ${sortedSalesRdd.partitioner}")
  val partitionContents = sortedSalesRdd.mapPartitionsWithIndex {
    (partitionId, iterator) =>
      iterator.map { case (key, value) => (partitionId, key, value) }.toList.iterator
  }
  println(s"Distribution of keys across $numPartitions2 partitions:")
  partitionContents.collect().foreach(println)
  println("   Spark SAMPLED the keys to find a cut-off that makes both partitions about the same size.")
  println("   global order = ranges between partitions + a local sort inside each partition.")
  println("   only the KEY is sorted: look at the amounts of Electronics (500, 1300, 300).")

  sub("CUSTOM partitioning: your own rule (exercise 5: partition web logs by Top-Level Domain)")
  // (Domain, LogLine)
  val logData = List(
    ("www.google.com", "GET /search 200"), ("www.vub.be", "GET / 200"), ("www.facebook.com", "POST /login 200"),
    ("research.vub.be", "GET /papers 200"), ("ads.google.com", "GET /adclick 200"), ("belgium.be", "GET /info 200"),
    ("wikipedia.org", "GET /wiki/Spark 200"), ("spark.apache.org", "GET / 200"), ("github.com", "GET /spark 200"),
    ("sourceforge.net", "GET /projects 200"), ("europa.eu", "GET / 200"), ("my.vub.ac.be", "GET / 200"),
    ("php.net", "GET / 200"), ("redis.io", "GET / 200"), ("en.wikipedia.org", "GET /wiki/RDD 200"), ("speedtest.net", "POST /results 200")
  )
  val logRdd = sc2.parallelize(logData)
  val tldPartitionedRdd = logRdd.partitionBy(new TldPartitioner(4))
  val tldContents = tldPartitionedRdd.mapPartitionsWithIndex {
    (partitionId, iterator) =>
      iterator.map { case (key, value) => (partitionId, key) }
  }
  println("--- Data distribution after custom TldPartitioner ---")
  tldContents.collect().foreach(println)
  println("   all .be domains share a partition, all .com domains too, ... and the KEY is still the full domain,")
  println("   so the RDD can still be joined on domain. A HashPartitioner on the domain could never do that.")
}

// -----------------------------------------------------------------------------------------------------
// 19. NARROW AND WIDE TRANSFORMATIONS
//     The cost of a transformation depends on the dependency between parent and child partitions.
//       narrow: each parent partition is used by AT MOST ONE child partition  -> no shuffle, pipelining
//       wide  : a parent partition is scattered over MULTIPLE child partitions -> shuffle
// -----------------------------------------------------------------------------------------------------
def s19_narrowAndWide(): Unit = section(19, "Narrow and wide dependencies, pipelining") {
  val sc = new SparkContext()

  sub("narrow: child partition i is computed from parent partition i, on the same worker")
  val a = sc.parallelize(1 to 8, 4)
  val b = a.map(_ * 10).filter(_ > 20)
  println("   A:");                       showPartitions(a)
  println("   B = A.map(...).filter(...):"); showPartitions(b)
  println("   C = A.union(B): also narrow, the partitions of A and B are simply put next to each other")
  showPartitions(a.union(b))

  sub("PIPELINING: Scala collections are eager and build every intermediate collection ...")
  val numbers = List(1, 2, 3, 4, 5)
  // creates a new, complete intermediate list: List(1, 4, 9, 16, 25)
  val squares = numbers.map { n => println(s"   map($n)"); n * n }
  // creates the final list from the intermediate list
  val largeSquares = squares.filter { sq => println(s"   filter($sq)"); sq > 10 }
  println(s"   result: $largeSquares   (ALL maps ran first, then all filters)")

  sub("... Spark fuses narrow transformations: each element goes through the WHOLE chain in one pass")
  val numbersRdd = sc.parallelize(1 to 5, 1)
  // logical plan (lineage) created, but does nothing yet
  val largeSquaresRdd = numbersRdd.map { n => println(s"map($n)"); n * n }
                                  .filter { sq => println(s"filter($sq)"); sq > 10 }
  // action triggers the pipelined execution
  println("   result: " + largeSquaresRdd.collect().mkString(", ") + "   (map and filter interleaved: no intermediate RDD [1, 4, 9, 16, 25] ever existed)")

  sub("which transformations are wide? (computed from the real lineage of each result)")
  val pairs = sc.parallelize(Seq(("a", 1), ("b", 2), ("a", 3), ("c", 4)), 2)
  val other = sc.parallelize(Seq(("a", "x"), ("b", "y")), 2)
  val nums  = sc.parallelize(Seq(3, 1, 2, 3), 2)
  def wideDeps(r: RDD[?], stopAt: Set[Int]): (Int, Int) =                 // (shuffles, all-to-all dependencies) added on top of the inputs
    if (stopAt(r.id)) (0, 0) else r.deps.map { d =>
      val (s, c) = wideDeps(d.rdd, stopAt)
      d match { case _: ShuffleDependency[?, ?] => (s + 1, c); case n: NarrowDependency => (s, c + (if (n.allToAll) 1 else 0)) }
    }.foldLeft((0, 0)) { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) }
  val inputs = Set(pairs.id, other.id, nums.id)
  val table: List[(String, RDD[?])] = List(
    "map" -> nums.map(_ + 1), "filter" -> nums.filter(_ > 1), "flatMap" -> nums.flatMap(n => Seq(n, n)),
    "mapValues" -> pairs.mapValues(_ + 1), "union" -> nums.union(nums), "coalesce(1)" -> nums.coalesce(1),
    "groupByKey" -> pairs.groupByKey(), "groupBy" -> nums.groupBy(_ % 2), "cogroup" -> pairs.cogroup(other),
    "reduceByKey" -> pairs.reduceByKey(_ + _), "foldByKey" -> pairs.foldByKey(0)(_ + _), "aggregateByKey" -> pairs.aggregateByKey(0)(_ + _, _ + _),
    "sortBy" -> nums.sortBy(n => n), "sortByKey" -> pairs.sortByKey(),
    "join" -> pairs.join(other), "leftOuterJoin" -> pairs.leftOuterJoin(other), "rightOuterJoin" -> pairs.rightOuterJoin(other),
    "fullOuterJoin" -> pairs.fullOuterJoin(other), "cartesian" -> nums.cartesian(nums),
    "repartition(4)" -> nums.repartition(4), "coalesce(1, shuffle = true)" -> nums.coalesce(1, shuffle = true), "distinct" -> nums.distinct(),
    "subtract" -> nums.subtract(nums), "intersection" -> nums.intersection(nums))
  table.foreach { case (name, rdd) =>
    val verdict = wideDeps(rdd, inputs) match {
      case (0, 0) => "narrow"
      case (0, _) => "WIDE   (every parent partition is combined with every partition of the other RDD)"
      case (s, _) => s"WIDE   ($s shuffle${if (s > 1) "s" else ""})"
    }
    println(f"   $name%-28s $verdict")
  }
  println("   join shows 2 shuffles because NEITHER input has a partitioner here (section 21 shows how to get 0).")
}

// -----------------------------------------------------------------------------------------------------
// 20. PARTITIONER PRESERVATION (Pair RDDs)
//     The partitioner is METADATA: it tells Spark "this data is organised by key", so future shuffles can
//     be skipped. Question for every transformation: does the child keep the parent's partitioner?
//       narrow + keys cannot change + partitions untouched  -> kept     (mapValues, flatMapValues, filter)
//       narrow but Spark cannot be sure about the keys       -> dropped  (map, flatMap, even if you keep the key!)
//       union -> kept only if both parents have the SAME partitioner; coalesce -> dropped (partitions merged)
//       wide with a real shuffle -> a NEW partitioner; wide on co-partitioned data -> inherited, no shuffle
// -----------------------------------------------------------------------------------------------------
def s20_partitionerPreservation(): Unit = section(20, "Partitioner preservation") {
  val sc = new SparkContext()
  val base = sc.parallelize(salesData10, 4).partitionBy(new HashPartitioner(4))
  val sameP  = sc.parallelize(salesData, 4).partitionBy(new HashPartitioner(4))
  val otherP = sc.parallelize(salesData, 4).partitionBy(new HashPartitioner(3))
  def row(label: String, rdd: RDD[?]): Unit = println(f"   $label%-52s ${rdd.partitioner.fold("None  (lost)")(p => "Some(" + p + ")")}")
  row("base = pairs.partitionBy(new HashPartitioner(4))", base)
  row("base.mapValues(_ + 1)", base.mapValues(_ + 1))
  row("base.flatMapValues(v => Seq(v, v))", base.flatMapValues(v => Seq(v, v)))
  row("base.filter(_._2 > 100)", base.filter(_._2 > 100))
  row("base.map { case (k, v) => (k, v + 1) }   same keys!", base.map { case (k, v) => (k, v + 1) })
  row("base.union(same partitioner)", base.union(sameP))
  row("base.union(different partitioner)", base.union(otherP))
  row("base.coalesce(2)", base.coalesce(2))
  row("base.reduceByKey(_ + _)       (no shuffle needed)", base.reduceByKey(_ + _))
  row("base.groupByKey()             (no shuffle needed)", base.groupByKey())
  row("base.sortByKey()              (new: range)", base.sortByKey())
  row("base.repartition(4)", base.repartition(4))

  sub("the co-partitioned join of the chapter: the result inherits the partitioner of its parents")
  val partitioner = new HashPartitioner(2);
  val a = sc.parallelize(Seq((1, "A"), (2, "B"))).partitionBy(partitioner)
  val b = sc.parallelize(Seq((1, "x"), (2, "y"))).partitionBy(partitioner)
  val joined = a.join(b)
  println(joined.partitioner.equals(a.partitioner))
  println(joined.partitioner.equals(b.partitioner))
}

// -----------------------------------------------------------------------------------------------------
// 21. SHUFFLING
//     A shuffle physically redistributes data over the network so that related records (same key) end up
//     together. It is THE most expensive operation: disk I/O + network I/O + (de)serialisation, and it
//     breaks pipelining. The RDD after a shuffle has a completely new set of partitions.
// -----------------------------------------------------------------------------------------------------
def s21_shuffling(): Unit = section(21, "Shuffling: detecting it, shuffle files, avoiding it") {
  val sc = new SparkContext()
  val pairs = sc.parallelize(Seq(("a", 1), ("b", 2), ("a", 3), ("c", 4), ("b", 5), ("a", 6)), 3)

  sub("how do you know a shuffle happens? (1) the runtime type ShuffledRDD, (2) toDebugString")
  val reduced = pairs.reduceByKey(_ + _)
  println("   pairs.mapValues(_ + 1) is a " + pairs.mapValues(_ + 1).kind + ", pairs.reduceByKey(_ + _) is a " + reduced.kind)
  println(reduced.toDebugString)

  sub("shuffle files: map tasks write their output, split per target partition, to their worker's local disk")
  sc.logJobs = true
  println("   result: " + reduced.collect().mkString(", "))
  sc.showShuffleFiles()
  println("   the tasks of the next stage FETCH their bucket from the files of all map tasks.")

  sub("shuffle files stay available: a later action on the same shuffle skips the stage before it")
  println("   count: " + reduced.count())
  sc.logJobs = false

  sub("AVOIDING shuffles. A join of two RDDs without partitioner: both sides are shuffled")
  val a = sc.parallelize(Seq((1, "A"), (2, "B")))
  val b = sc.parallelize(Seq((1, "x"), (2, "y")))
  val joined = a.join(b)
  println(joined.toDebugString)
  sc.measure("join, no partitioners") { joined.collect() }

  sub("co-partitioned join: both sides use the SAME partitioner -> keys are already co-located -> no shuffle")
  val a2 = sc.parallelize(Seq((1, "A"), (2, "B"))).partitionBy(new HashPartitioner(2)).persist()
  val b2 = sc.parallelize(Seq((1, "x"), (2, "y"))).partitionBy(new HashPartitioner(2)).persist()
  sc.measure("partitionBy both sides (paid once, then persisted)") { a2.count(); b2.count() }
  val joined2 = a2.join(b2)
  println(joined2.toDebugString)
  println("   ^ the join (CoGroupedRDD) now hangs on BOTH parents with ' |  ' = narrow. The only '+-' left are the two")
  println("     partitionBy shuffles, and those are already done and cached.")
  sc.measure("co-partitioned join") { joined2.collect() }
  println("   the logical dependency is still 'wide', but it is satisfied without moving data: as cheap as narrow.")
  println("   co-partitioning = same Partitioner (logical).  co-location = same key on same worker (physical result).")
  println("   partitionBy is lazy: persist the result, or the partitioning work is repeated for every action.")
}

// -----------------------------------------------------------------------------------------------------
// 22. MINIMISING THE OVERHEAD OF SHUFFLES
//     When a shuffle cannot be avoided: move as little data as possible.
//       (a) pre-partition   (b) pre-aggregate before the shuffle = map-side combine   (c) filter early
// -----------------------------------------------------------------------------------------------------
def s22_minimizingShuffles(): Unit = section(22, "Minimising shuffles: pre-partitioning, map-side combine, filter early") {
  val sc = new SparkContext()
  // 5000 (word, 1) pairs in 4 partitions; only 3 distinct words
  val words = sc.parallelize(Seq.fill(1000)(Seq("Car", "Car", "Car", "River", "Deer")).flatten, 4).map(w => (w, 1))

  sub("(b) map-side combine: reduceByKey vs groupByKey (same result!)")
  val viaGroup  = sc.measure("groupByKey().mapValues(_.sum)") { words.groupByKey().mapValues(_.sum).collect() }
  val viaReduce = sc.measure("reduceByKey(_ + _)           ") { words.reduceByKey(_ + _).collect() }
  val viaAgg    = sc.measure("aggregateByKey(0)(_ + _, _ + _)") { words.aggregateByKey(0)(_ + _, _ + _).collect() }
  println(s"   ${viaGroup.sorted.mkString(", ")} = ${viaReduce.sorted.mkString(", ")} = ${viaAgg.sorted.mkString(", ")}")
  println("   groupByKey ships all 5000 pairs. reduceByKey first reduces INSIDE each partition and ships one pair")
  println("   per key per partition: 3 keys x 4 partitions = 12. aggregateByKey is built for this: seqOp runs")
  println("   before the shuffle (local), combOp after it (global).")

  sub("(a) pre-partitioning: partition ONE rdd by key once, later key-based operations on it are local")
  val pre = words.partitionBy(new HashPartitioner(4)).persist()
  sc.measure("partitionBy + persist (paid once)       ") { pre.count() }
  sc.measure("groupByKey  on the pre-partitioned RDD  ") { pre.groupByKey().count() }
  sc.measure("reduceByKey on the pre-partitioned RDD  ") { pre.reduceByKey(_ + _).count() }
  sc.measure("groupByKey  on the original RDD         ") { words.groupByKey().count() }

  sub("(c) filter early: what you remove before the shuffle never has to travel")
  val orders    = sc.parallelize((1 to 10000).map(i => (i % 1000, i)), 4)                         // (customerId, orderId)
  val customers = sc.parallelize((0 until 1000).map(i => (i, if (i % 10 == 0) "BE" else "NL")), 4) // (customerId, country)
  val late  = sc.measure("join, then filter          ") { orders.join(customers).filter { case (_, (order, country)) => country == "BE" && order > 9000 }.count() }
  val early = sc.measure("filter both sides, then join") { orders.filter(_._2 > 9000).join(customers.filter(_._2 == "BE")).count() }
  println(s"   same answer ($late = $early), a fraction of the shuffled data")

  sub("sidebar: shuffle files vs persist")
  val counts = words.reduceByKey(_ + _)
  sc.measure("no persist, 1st action") { counts.count() }
  sc.measure("no persist, 2nd action") { counts.count() }
  println("   ^ 2nd action: the stage before the shuffle is SKIPPED (shuffle files reused), only the fetch is redone")
  val cached = words.reduceByKey(_ + _).persist()
  sc.measure("persist,    1st action") { cached.count() }
  sc.measure("persist,    2nd action") { cached.count() }
  println("   ^ with persist Spark reads the cache and does not even look at the shuffle behind it.")
  println("   So persist does not always pay off locally (small shuffle files on a local disk are cheap to fetch),")
  println("   but on a real cluster, where shuffle files travel over the network, it usually does. You also control")
  println("   the storage level and replication of a persisted RDD; you control nothing about shuffle files.")
}

// -----------------------------------------------------------------------------------------------------
// 23. FAULT TOLERANCE
//     Partitions get lost when a worker crashes. Spark does not rerun the job and does not need replicas:
//     it recomputes ONLY the lost partitions from their lineage (transitively, back to the source).
//       narrow lineage: cheap, re-read one parent partition and re-apply the functions
//       wide lineage  : expensive, a lost partition depends on ALL parent partitions (shuffle)
// -----------------------------------------------------------------------------------------------------
def s23_faultTolerance(): Unit = section(23, "Fault tolerance: recomputing lost partitions from lineage") {
  sub("narrow transformations: the 3-node log example of the chapter")
  val sc = new SparkContext(numWorkers = 3)
  sc.writeFile("huge.log", (1 to 9).map(i => if (i % 3 == 0) s"CRITICAL disk $i" else s"INFO ok $i")); sc.blockSize = 3
  val logLines = sc.textFile("huge.log")
  val criticalLines = logLines.filter(_.contains("CRITICAL")).persist()
  println("   count = " + criticalLines.count() + "   (all 3 partitions computed and kept in memory, one per worker)")
  sc.crashWorker(1)
  sc.trace = true
  println("   count = " + criticalLines.count())
  sc.trace = false
  println("   ^ only partition 1 was rebuilt (re-read its block of huge.log, re-apply filter); partitions 0 and 2 came from memory.")

  sub("wide transformations: a lost partition AFTER a shuffle")
  val sc2 = new SparkContext(numWorkers = 3)
  val counts = sc2.parallelize(Seq("a", "b", "a", "c", "b", "a", "d", "a", "c"), 3).map(w => (w, 1)).reduceByKey(_ + _).persist()
  println("   counts = " + counts.collect().sorted.mkString(", "))
  sc2.crashWorker(1)                // loses: cached partition 1 of counts AND the shuffle file written by map task 1
  sc2.trace = true; sc2.logJobs = true
  println("   counts = " + counts.collect().sorted.mkString(", "))
  println("   ^ the map task whose shuffle file was lost had to run again; then reduce partition 1 re-fetched ALL shuffle files.")

  sub("the worst case: no shuffle files left at all -> every parent partition is recomputed and re-shuffled")
  sc2.crashWorker(0); sc2.crashWorker(1); sc2.crashWorker(2)
  println("   counts = " + counts.collect().sorted.mkString(", "))
  sc2.trace = false; sc2.logJobs = false

  sub("a replicated storage level (MEMORY_ONLY_2) survives a crash without ANY recomputation")
  val sc3 = new SparkContext(numWorkers = 3)
  val safe = sc3.parallelize(Seq("a", "b", "a", "c", "b", "a"), 3).map(w => (w, 1)).reduceByKey(_ + _).persist(StorageLevel.MEMORY_ONLY_2)
  safe.count()
  sc3.crashWorker(1)
  sc3.measure("after the crash") { safe.count() }
  println("   'persist after a wide transformation': the cache is a cheap recovery point, lineage behind it is not replayed.")
}

// -----------------------------------------------------------------------------------------------------
// 24. EXECUTION
//     Driver   : runs your main(), owns the SparkContext, plans the work. It does NOT process RDD data.
//     Executors: processes on the worker nodes that run the tasks and store the data.
//     Cluster manager (Standalone, YARN, Kubernetes): hands out resources, launches executors.
//
//     When you call an action:
//       1. lineage  : the full DAG of transformations is known
//       2. stages   : the DAG scheduler cuts the DAG at every shuffle; narrow chains are pipelined in one stage
//       3. tasks    : one task per partition for every stage
//       4. execution: tasks run on the workers; shuffle output of one stage is the input of the next;
//                     the final result goes back to the driver
// -----------------------------------------------------------------------------------------------------
def s24_execution(): Unit = section(24, "Execution: driver, executors, jobs, stages, tasks") {
  val sc = new SparkContext(numWorkers = 3)
  println(s"   this 'cluster': 1 driver + ${sc.numWorkers} workers (one executor each); partition p runs on worker p % ${sc.numWorkers}")

  sub("execution flow of a word count that ends with a filter")
  val lines  = sc.parallelize(Seq("to be or not", "to be that is", "the question to", "be or not"), 3)
  val counts = lines.flatMap(_.split(" ")).map(w => (w, 1)).reduceByKey(_ + _).filter(_._2 > 1)
  println("   1. the lineage:")
  println(counts.toDebugString)
  println("   2 + 3. the stages and tasks Spark derives from it:")
  sc.showStages(counts)
  println("   4. execution (the driver log):")
  sc.logJobs = true
  val result = counts.collect()
  sc.logJobs = false
  println("   result back in the driver: " + result.sorted.mkString(", "))

  sub("WHERE does code run? foreach(println): the function is shipped to the workers and runs there")
  sc.writeFile("huge.log", Seq("INFO a", "CRITICAL b", "INFO c", "CRITICAL d", "INFO e", "CRITICAL f")); sc.blockSize = 2
  val logLines = sc.textFile("huge.log")
  val criticalLines = logLines.filter(_.contains("CRITICAL"))
  criticalLines.foreach(println)
  println("   ^ printed on the workers: in the executor logs, not on the driver's console (unless local mode)")

  sub("collect().foreach(println): collect brings the data to the driver, then Scala's foreach prints there")
  criticalLines.collect().foreach(println)
}

// -----------------------------------------------------------------------------------------------------
// 25. DISTRIBUTING TRANSFORMATIONS AND ACTIONS
//     Transformations are easy to distribute (data parallelism: ship a small function to the data).
//     Actions that reduce everything to ONE value are harder: partial results of the workers must be
//     combined by the driver. Inherently sequential operations (foldLeft) are therefore not offered.
// -----------------------------------------------------------------------------------------------------
def s25_distributingActions(): Unit = section(25, "Distributing actions: foldLeft vs fold vs aggregate") {
  val sc = new SparkContext()
  val words = sc.parallelize(Seq("spark", "is", "lazy", "and", "fast"), 2)

  sub("def foldLeft(z: B)(op: (B, A) => B): B     exists on Scala collections, NOT on RDDs")
  println("   List: total length = " + List("spark", "is", "lazy", "and", "fast").foldLeft(0)((acc, w) => acc + w.length))
  println("   does `words.foldLeft(0)((acc, w) => acc + w.length)` compile for an RDD? " +
    scala.compiletime.testing.typeChecks("words.foldLeft(0)((acc, w) => acc + w.length)"))
  println("   why: every worker would return a B (an Int). op is (B, A) => B, so nobody can combine two B's.")

  sub("def fold(z: A)(op: (A, A) => A): A     result type = element type, so op can also combine partial results")
  println("   longest word = " + words.fold("")((w1, w2) => if (w1.length >= w2.length) w1 else w2))
  println("   op must be commutative and associative: the order of combining is not guaranteed.")

  sub("def aggregate[B](z: B)(seqop: (B, A) => B, combop: (B, B) => B): B     the general solution")
  val totalLength = words.aggregate(0)((acc, w) => acc + w.length, _ + _)     // seqop on the workers, combop on the driver
  println("   total length = " + totalLength)
  val (sum, count) = sc.parallelize(Seq(4, 8, 15, 16, 23, 42), 3).aggregate((0, 0))((acc, n) => (acc._1 + n, acc._2 + 1), (a, b) => (a._1 + b._1, a._2 + b._2))
  println(s"   average via aggregate: sum = $sum, count = $count, avg = ${sum.toDouble / count}")
  println("   aggregate (action) returns ONE value to the driver. aggregateByKey (transformation) returns a new")
  println("   distributed RDD with one value per key, for use in later stages.")
}

// -----------------------------------------------------------------------------------------------------
// 26. EXERCISES of the chapter, on small made-up data (exercise 5, the custom partitioner, is in section 18)
// -----------------------------------------------------------------------------------------------------
def s26_exercises(): Unit = section(26, "Exercises") {
  val sc = new SparkContext()
  sub("Exercise 1: word count")
  sc.writeFile("hamlet.txt", Seq(
    "To be, or not to be, that is the question:", "Whether 'tis nobler in the mind to suffer",
    "The slings and arrows of outrageous fortune,", "Or to take arms against a sea of troubles",
    "And by opposing end them. To die: to sleep;", "No more; and by a sleep to say we end"))
  val wordCounts = sc.textFile("hamlet.txt").flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
  wordCounts.sortBy(-_._2).take(8).foreach(println)
  println("   efficiency: flatMap and map are narrow (pipelined in one stage). The one unavoidable shuffle uses")
  println("   reduceByKey, so counts are pre-aggregated per partition before travelling (map-side combine).")

  // wiki logs: "language pageName accessCount contentSize"
  sc.blockSize = 4                                       // 12 lines -> 3 blocks -> 3 partitions
  sc.writeFile("wiki-logs-full.txt", Seq(
    "en Main_Page 500 20000", "en Apache_Spark 120 9000", "en.mw Special:Search 300 100", "nl Hoofdpagina 90 15000",
    "nl Apache_Spark 30 4000", "nl.voy Brussel 25 3000", "es Wikipedia:Portada 260 18000", "es.mw Especial:Buscar 60 100",
    "fr Accueil 200 17000", "en Scala_(programming_language) 80 7000", "de Hauptseite 150 16000", "es Apache_Spark 20 5000"))
  case class Log(language: String, page: String, accesses: Long, size: Long)
  val logs = sc.textFile("wiki-logs-full.txt").map(_.split(" ")).map(p => Log(p(0), p(1), p(2).toLong, p(3).toLong))

  sub("Exercise 2: English pages")
  val english = logs.filter(_.language == "en").persist()        // used by three actions -> persist
  println("   Number of different pages accessed: " + english.count())                      // pageName is unique per record
  println("   Total number of page accesses: " + english.map(_.accesses).reduce(_ + _))
  println("   Most accessed page: " + english.reduce((a, b) => if (a.accesses >= b.accesses) a else b).page)

  sub("Exercise 3: full wiki logs")
  println("   Total number of accesses to Dutch pages: " + logs.filter(_.language.startsWith("nl")).map(_.accesses).reduce(_ + _))
  val perLanguage = logs.filter(!_.language.startsWith("en"))                        // filter early
    .map(l => (l.language.takeWhile(_ != '.'), l.accesses))                         // "es.mw" -> "es"
    .reduceByKey(_ + _)                                                              // map-side combine
  println("   Second-most popular language in terms of page accesses: " + perLanguage.reduce((a, b) => if (a._2 >= b._2) a else b)._1)

  sub("Exercise 4: persisting an RDD that is used by three actions")
  def languageTotals = logs.map(l => (l.language.takeWhile(_ != '.'), l.accesses)).reduceByKey(_ + _)
  def threeActions(totals: RDD[(String, Long)]): Unit = {
    sc.measure("1. number of languages  ") { totals.count() }
    sc.measure("2. accesses to Dutch    ") { totals.lookup("nl") }
    sc.measure("3. top language but 'en'") { totals.filter(_._1 != "en").reduce((a, b) => if (a._2 >= b._2) a else b) }
  }
  println("   without persist:"); threeActions(languageTotals)
  println("   with persist:");    threeActions(languageTotals.persist())
  println("   answers: action 1 fills the cache; actions 2 and 3 reuse it, so the shuffle output is fetched only once.")
  println("   'missing stages': without persist the map stage is skipped in actions 2 and 3 because its shuffle files")
  println("   still exist; with persist it is not even planned, the cached partitions are read directly.")

  sub("Rules of thumb of this chapter")
  println("""   - transformations are lazy, actions trigger jobs; reuse an RDD -> persist it
   - narrow = pipelined in one stage; wide = shuffle = stage boundary = expensive
   - prefer reduceByKey / aggregateByKey over groupByKey (map-side combine)
   - filter early; partitionBy + persist to co-partition RDDs that you join repeatedly
   - mapValues / filter keep the partitioner, map drops it
   - Spark does NOT do these optimisations for you with RDDs: you are the optimiser (chapter 3 changes that)""")
}

// prints the content of every partition, like the mapPartitionsWithIndex trick used in the chapter
def showPartitions[T](rdd: RDD[T]): Unit = rdd.sc.quietly {
  rdd.sc.runJob(rdd, "showPartitions")(_.toVector).zipWithIndex.foreach { case (elems, p) =>
    println(f"      partition $p (worker-${p % rdd.sc.numWorkers}): ${elems.mkString(", ")}")
  }
}

// =====================================================================================================
//  PART 2 - MINI-SPARK, the engine behind the demos. Worth reading: this is how Spark works, in small.
//           Reading order: RDD -> MapPartitionsRDD -> ShuffleDependency / ShuffledRDD -> SparkContext.runJob
// =====================================================================================================

// ----- storage levels (section 16) ---------------------------------------------------------------------
final case class StorageLevel(name: String, useDisk: Boolean, useMemory: Boolean, useOffHeap: Boolean, deserialized: Boolean, replication: Int) {
  override def toString: String = name
  def description: String = {
    val where =
      if (name == "NONE") "not persisted"
      else if (useOffHeap) "outside the JVM heap (no garbage collection, manual memory management)"
      else if (useMemory && useDisk) "in memory, spills to disk when it does not fit"
      else if (useMemory) "in memory only (what does not fit is recomputed when needed)"
      else "on disk only"
    val form = if (name == "NONE" || useOffHeap) "" else if (deserialized && useMemory) ", as plain JVM objects" else ", serialized (smaller, costs CPU)"
    val copies = if (replication > 1) s", $replication copies on different nodes (fast recovery, ${replication}x the space)" else ""
    where + form + copies
  }
}
object StorageLevel {
  val NONE                  = StorageLevel("NONE", false, false, false, false, 1)
  val DISK_ONLY             = StorageLevel("DISK_ONLY", true, false, false, false, 1)
  val DISK_ONLY_2           = DISK_ONLY.copy(name = "DISK_ONLY_2", replication = 2)
  val DISK_ONLY_3           = DISK_ONLY.copy(name = "DISK_ONLY_3", replication = 3)
  val MEMORY_ONLY           = StorageLevel("MEMORY_ONLY", false, true, false, true, 1)
  val MEMORY_ONLY_2         = MEMORY_ONLY.copy(name = "MEMORY_ONLY_2", replication = 2)
  val MEMORY_ONLY_SER       = StorageLevel("MEMORY_ONLY_SER", false, true, false, false, 1)
  val MEMORY_ONLY_SER_2     = MEMORY_ONLY_SER.copy(name = "MEMORY_ONLY_SER_2", replication = 2)
  val MEMORY_AND_DISK       = StorageLevel("MEMORY_AND_DISK", true, true, false, true, 1)
  val MEMORY_AND_DISK_2     = MEMORY_AND_DISK.copy(name = "MEMORY_AND_DISK_2", replication = 2)
  val MEMORY_AND_DISK_SER   = StorageLevel("MEMORY_AND_DISK_SER", true, true, false, false, 1)
  val MEMORY_AND_DISK_SER_2 = MEMORY_AND_DISK_SER.copy(name = "MEMORY_AND_DISK_SER_2", replication = 2)
  val OFF_HEAP              = StorageLevel("OFF_HEAP", true, true, true, false, 1)
  val all = List(NONE, DISK_ONLY, DISK_ONLY_2, DISK_ONLY_3, MEMORY_ONLY, MEMORY_ONLY_2, MEMORY_ONLY_SER, MEMORY_ONLY_SER_2,
    MEMORY_AND_DISK, MEMORY_AND_DISK_2, MEMORY_AND_DISK_SER, MEMORY_AND_DISK_SER_2, OFF_HEAP)
}

// ----- partitioners (section 18) -----------------------------------------------------------------------
abstract class Partitioner {
  def numPartitions: Int
  def getPartition(key: Any): Int          // THE rule: which partition does a pair with this key belong to?
}

class HashPartitioner(partitions: Int) extends Partitioner {
  def numPartitions: Int = partitions
  def getPartition(key: Any): Int = {
    val mod = (if (key == null) 0 else key.hashCode) % numPartitions
    mod + (if (mod < 0) numPartitions else 0)
  }
  override def equals(other: Any): Boolean = other match { case h: HashPartitioner => h.numPartitions == numPartitions; case _ => false }
  override def hashCode: Int = numPartitions
  override def toString: String = s"org.apache.spark.HashPartitioner@${numPartitions.toHexString}"
}

class RangePartitioner[K, V](partitions: Int, rdd: RDD[(K, V)], val ascending: Boolean = true)(using ord: Ordering[K]) extends Partitioner {
  // The cut-off points between the ranges. Spark finds them by SAMPLING the keys of the rdd (which costs a job!);
  // this mini version simply looks at all keys. The goal: partitions of roughly equal size.
  lazy val rangeBounds: Vector[K] = {
    val keys = rdd.sc.runJob(rdd, "sample the keys (RangePartitioner)")(_.map(_._1).toVector).flatten.sorted(using ord)
    if (keys.isEmpty) Vector() else (1 until partitions).map(i => keys(math.max(0, i * keys.size / partitions - 1))).distinct.toVector
  }
  def numPartitions: Int = rangeBounds.size + 1
  def getPartition(key: Any): Int = {
    val p = rangeBounds.count(bound => ord.gt(key.asInstanceOf[K], bound))      // how many bounds lie below this key?
    if (ascending) p else rangeBounds.size - p
  }
  override def equals(other: Any): Boolean = other match { case r: RangePartitioner[?, ?] => r.rangeBounds == rangeBounds && r.ascending == ascending; case _ => false }
  override def hashCode: Int = rangeBounds.hashCode
  override def toString: String = s"org.apache.spark.RangePartitioner(bounds: ${rangeBounds.mkString(" | ")})"
}

// ----- dependencies: the edges of the lineage graph (section 19) ---------------------------------------
sealed trait Dependency { def rdd: RDD[?] }

// narrow: a child partition needs a known, small set of parent partitions. No data is redistributed.
final class NarrowDependency(val rdd: RDD[?], val allToAll: Boolean = false) extends Dependency

// wide: the parent's records are re-distributed over the child's partitions according to a partitioner.
final class ShuffleDependency[K, V](val rdd: RDD[(K, V)], val partitioner: Partitioner, defaultOp: String,
                                    mapSideCombine: Option[Iterator[(K, V)] => Iterator[(K, Any)]]) extends Dependency {
  val sc: SparkContext = rdd.sc
  val shuffleId: Int = sc.newShuffleId()
  val op: String = sc.callSite.getOrElse(defaultOp)

  // MAP SIDE (runs as a task of the stage BEFORE the shuffle): compute one parent partition, optionally
  // pre-aggregate it (map-side combine), split the records per target partition, write them as shuffle files.
  def runMapTask(mapPartition: Int): Unit = {
    val records: Iterator[(K, Any)] = mapSideCombine match {
      case Some(combine) => combine(rdd.iterator(mapPartition))
      case None          => rdd.iterator(mapPartition)
    }
    val buckets = Array.fill(partitioner.numPartitions)(Vector.newBuilder[(Any, Any)])
    records.foreach { case (k, v) => buckets(partitioner.getPartition(k)) += ((k, v)); sc.inc("records shuffled") }
    sc.shuffleFiles((shuffleId, mapPartition)) = buckets.map(_.result())
  }
  // REDUCE SIDE (a task of the stage AFTER the shuffle): fetch "my" bucket from the shuffle files of ALL map tasks.
  def fetch(reducePartition: Int): Iterator[(K, Any)] =
    (0 until rdd.numPartitions).iterator.flatMap(m => sc.shuffleFiles((shuffleId, m))(reducePartition).iterator).asInstanceOf[Iterator[(K, Any)]]
}

// ----- the RDD ------------------------------------------------------------------------------------------
// An RDD does not hold data. It holds: its partitions, its dependencies on parent RDDs, a function to compute
// a partition from the parents (compute), and optionally a partitioner. That is all Spark needs.
abstract class RDD[T](val sc: SparkContext, val deps: List[Dependency], defaultOp: String) {
  val id: Int = sc.newRddId()
  val op: String = sc.callSite.getOrElse(defaultOp)          // the operation that created this RDD
  def kind: String = getClass.getSimpleName
  def numPartitions: Int
  def getNumPartitions: Int = numPartitions
  def partitioner: Option[Partitioner] = None
  def compute(split: Int): Iterator[T]                       // how to produce partition `split` (lazily: an Iterator!)

  // every read of a partition goes through here: use the cache when the RDD is persisted, otherwise (re)compute
  final def iterator(split: Int): Iterator[T] = if (storageLevel == StorageLevel.NONE) computeTraced(split) else sc.getOrCompute(this, split)
  def computeTraced(split: Int): Iterator[T] = { if (sc.trace) println(s"computing partition $split of $kind[$id] ($op)"); compute(split) }

  // --- persistence (section 16) ---
  var storageLevel: StorageLevel = StorageLevel.NONE
  def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY): this.type = { storageLevel = newLevel; this }   // lazy: only a flag!
  def cache(): this.type = persist(StorageLevel.MEMORY_ONLY)
  def unpersist(blocking: Boolean = false): this.type = { storageLevel = StorageLevel.NONE; sc.dropBlocks(id); this }

  // --- transformations: each one just creates a new RDD object that points to its parent(s). Nothing runs. ---
  def map[U](f: T => U): RDD[U] = new MapPartitionsRDD[U, T](this, "map", (_, it) => it.map(f))
  def flatMap[U](f: T => IterableOnce[U]): RDD[U] = new MapPartitionsRDD[U, T](this, "flatMap", (_, it) => it.flatMap(f))
  def filter(f: T => Boolean): RDD[T] = new MapPartitionsRDD[T, T](this, "filter", (_, it) => it.filter(f), preservesPartitioning = true)
  def mapPartitionsWithIndex[U](f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean = false): RDD[U] =
    new MapPartitionsRDD[U, T](this, "mapPartitionsWithIndex", f, preservesPartitioning)
  def distinct(): RDD[T] = sc.withCallSite("distinct") { map(x => (x, null)).reduceByKey((x, _) => x).keys }
  def groupBy[K](f: T => K): RDD[(K, Iterable[T])] = sc.withCallSite("groupBy") { map(x => (f(x), x)).groupByKey() }
  def sortBy[K](f: T => K, ascending: Boolean = true)(using Ordering[K]): RDD[T] = sc.withCallSite("sortBy") { map(x => (f(x), x)).sortByKey(ascending).values }
  def union(other: RDD[T]): RDD[T] = new UnionRDD(sc, List(this, other))
  def intersection(other: RDD[T]): RDD[T] = sc.withCallSite("intersection") {
    map(x => (x, null)).cogroup(other.map(x => (x, null))).filter { case (_, (mine, theirs)) => mine.nonEmpty && theirs.nonEmpty }.keys }
  def subtract(other: RDD[T]): RDD[T] = sc.withCallSite("subtract") {
    map(x => (x, null)).cogroup(other.map(x => (x, null))).flatMap { case (x, (mine, theirs)) => if (theirs.isEmpty) mine.map(_ => x) else Nil } }
  def cartesian[U](other: RDD[U]): RDD[(T, U)] = new CartesianRDD(this, other)
  def repartition(numPartitions: Int): RDD[T] = coalesce(numPartitions, shuffle = true)
  def coalesce(numPartitions: Int, shuffle: Boolean = false): RDD[T] =
    if (!shuffle) new CoalescedRDD(this, numPartitions)
    else sc.withCallSite(if (sc.callSite.isEmpty) "repartition" else "coalesce") {
      // give every record a running number as key, hash-partition on it (round robin = balanced), drop the key again
      val parents = this.numPartitions
      mapPartitionsWithIndex((index, it) => { var position = index * numPartitions / parents; it.map { x => position += 1; (position, x) } })
        .partitionBy(new HashPartitioner(numPartitions)).values
    }

  // --- actions: each one submits a job to the scheduler and returns a real value to the driver ---
  def collect()(using ClassTag[T]): Array[T] = sc.runJob(this, "collect")(_.toVector).flatten.toArray
  def count(): Long = sc.runJob(this, "count")(_.size.toLong).sum
  def take(num: Int)(using ClassTag[T]): Array[T] = {          // scans as few partitions as possible: 1, then 4 more, ...
    val result = mutable.ArrayBuffer[T](); var scanned = 0; var batch = 1
    while (result.size < num && scanned < numPartitions) {
      val parts = scanned until math.min(numPartitions, scanned + batch)
      val missing = num - result.size
      sc.runJobOn(this, s"take($num)", parts)(_.take(missing).toVector).foreach(found => result ++= found.take(num - result.size))
      scanned += parts.size; batch *= 4
    }
    result.toArray
  }
  def reduce(f: (T, T) => T): T = {
    val partials = sc.runJob(this, "reduce")(_.reduceOption(f))                // every task reduces ITS partition ...
    partials.flatten.reduceOption(f).getOrElse(throw new UnsupportedOperationException("empty collection"))   // ... the driver combines
  }
  def fold(zeroValue: T)(op: (T, T) => T): T = sc.runJob(this, "fold")(_.foldLeft(zeroValue)(op)).foldLeft(zeroValue)(op)
  def aggregate[U](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U =
    sc.runJob(this, "aggregate")(_.foldLeft(zeroValue)(seqOp)).foldLeft(zeroValue)(combOp)      // seqOp in the tasks, combOp in the driver
  def foreach(f: T => Unit): Unit = { sc.runJob(this, "foreach")(_.foreach(f)); () }            // f runs inside the tasks = on the workers
  def takeSample(withReplacement: Boolean, num: Int, seed: Long = 7L)(using ClassTag[T]): Array[T] = {
    val all = sc.runJob(this, "takeSample")(_.toVector).flatten; val random = new scala.util.Random(seed)
    if (all.isEmpty) Array.empty[T] else if (withReplacement) Array.fill(num)(all(random.nextInt(all.size))) else random.shuffle(all).take(num).toArray
  }
  def takeOrdered(num: Int)(implicit ord: Ordering[T], tag: ClassTag[T]): Array[T] =
    sc.runJob(this, "takeOrdered")(_.toVector.sorted(using ord).take(num)).flatten.sorted(using ord).take(num).toArray   // top-n per partition, then merge
  def saveAsTextFile(path: String): Unit = {
    sc.runJob(this, "saveAsTextFile")(_.map(_.toString).toVector).zipWithIndex.foreach { case (lines, p) => sc.fs(f"$path/part-$p%05d") = lines }
    sc.fs(s"$path/_SUCCESS") = Vector()
  }

  // --- the lineage as text (section 15). " |  " = narrow dependency, " +-" = shuffle dependency ---
  def toDebugString: String = {
    def describe(r: RDD[?]): String = s"${r.kind}[${r.id}] at ${r.op}" + (if (r.storageLevel != StorageLevel.NONE) s" [persisted: ${r.storageLevel}]" else "")
    def lines(r: RDD[?], first: String, rest: String): List[String] = (first + describe(r)) :: r.deps.flatMap {
      case s: ShuffleDependency[?, ?] => lines(s.rdd, rest + s" +-(${s.rdd.numPartitions}) ", rest + "   ")
      case n                          => lines(n.rdd, rest + " |  ", rest)
    }
    lines(this, s"($numPartitions) ", "").mkString("\n")
  }
}

// one-to-one narrow dependency: child partition i = f(parent partition i). Used by map, filter, flatMap, mapValues, ...
class MapPartitionsRDD[U, T](prev: RDD[T], defaultOp: String, f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean = false)
    extends RDD[U](prev.sc, List(new NarrowDependency(prev)), defaultOp) {
  def numPartitions: Int = prev.numPartitions
  override def partitioner: Option[Partitioner] = if (preservesPartitioning) prev.partitioner else None    // section 20
  def compute(split: Int): Iterator[U] = f(split, prev.iterator(split))     // iterators chain lazily = PIPELINING
}

class ParallelCollectionRDD[T](sc0: SparkContext, data: IndexedSeq[T], numSlices: Int) extends RDD[T](sc0, Nil, "parallelize") {
  def numPartitions: Int = numSlices
  def compute(split: Int): Iterator[T] =                                    // contiguous slices, original order kept
    data.slice((split.toLong * data.length / numSlices).toInt, ((split + 1L) * data.length / numSlices).toInt).iterator
}

class HadoopRDD(sc0: SparkContext, path: String, minPartitions: Int) extends RDD[String](sc0, Nil, "textFile") {
  private def lines: Vector[String] = sc.fs.getOrElse(path, throw new java.io.FileNotFoundException(s"Input path does not exist: $path"))
  def numPartitions: Int = math.max(1, math.ceil(lines.size.toDouble / sc.blockSize).toInt)               // one partition per block
  def compute(split: Int): Iterator[String] =                               // the file is only touched HERE, when a task runs
    lines.slice(split * sc.blockSize, (split + 1) * sc.blockSize).iterator.map { line => sc.inc("lines read from file"); line }
}

class UnionRDD[T](sc0: SparkContext, rdds: List[RDD[T]]) extends RDD[T](sc0, rdds.map(r => new NarrowDependency(r)), "union") {
  private val samePartitioner = rdds.forall(_.partitioner.isDefined) && rdds.map(_.partitioner).distinct.size == 1
  override def partitioner: Option[Partitioner] = if (samePartitioner) rdds.head.partitioner else None
  def numPartitions: Int = if (samePartitioner) rdds.head.numPartitions else rdds.map(_.numPartitions).sum
  def compute(split: Int): Iterator[T] =
    if (samePartitioner) rdds.iterator.flatMap(_.iterator(split))           // partition i of every parent holds the same keys
    else { var i = split; val parent = rdds.find(r => if (i < r.numPartitions) true else { i -= r.numPartitions; false }).get; parent.iterator(i) }
}

class CoalescedRDD[T](prev: RDD[T], maxPartitions: Int) extends RDD[T](prev.sc, List(new NarrowDependency(prev)), "coalesce") {
  def numPartitions: Int = math.min(maxPartitions, prev.numPartitions)      // can only reduce; neighbouring partitions are merged
  def compute(split: Int): Iterator[T] =
    (0 until prev.numPartitions).filter(p => p * numPartitions / prev.numPartitions == split).iterator.flatMap(prev.iterator)
}

class CartesianRDD[A, B](left: RDD[A], right: RDD[B])
    extends RDD[(A, B)](left.sc, List(new NarrowDependency(left, allToAll = true), new NarrowDependency(right, allToAll = true)), "cartesian") {
  def numPartitions: Int = left.numPartitions * right.numPartitions         // EVERY left partition meets EVERY right partition
  def compute(split: Int): Iterator[(A, B)] = {
    val ys = right.iterator(split % right.numPartitions).toVector
    left.iterator(split / right.numPartitions).flatMap(x => ys.iterator.map(y => (x, y)))
  }
}

// how values of one key are combined: used by reduceByKey, groupByKey, aggregateByKey, ... (they only differ in these 3 functions)
final case class Aggregator[V, C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C)

def combineValuesByKey[K, V, C](records: Iterator[(K, V)], agg: Aggregator[V, C]): Iterator[(K, C)] = {
  val combined = mutable.LinkedHashMap[K, C]()
  records.foreach { case (k, v) => combined(k) = combined.get(k).fold(agg.createCombiner(v))(c => agg.mergeValue(c, v)) }
  combined.iterator
}
def combineCombinersByKey[K, C](records: Iterator[(K, C)], merge: (C, C) => C): Iterator[(K, C)] = {
  val combined = mutable.LinkedHashMap[K, C]()
  records.foreach { case (k, c) => combined(k) = combined.get(k).fold(c)(existing => merge(existing, c)) }
  combined.iterator
}

// the RDD that comes out of a shuffle. Its partitions are defined by the partitioner, not by the parent's layout.
class ShuffledRDD[K, V, C](prev: RDD[(K, V)], part: Partitioner, defaultOp: String, agg: Option[Aggregator[V, C]] = None,
                           mapSideCombine: Boolean = false, keyOrdering: Option[Ordering[K]] = None)
    extends RDD[(K, C)](prev.sc, List(new ShuffleDependency[K, V](prev, part, defaultOp,
      if (mapSideCombine) agg.map(a => (records: Iterator[(K, V)]) => combineValuesByKey(records, a)) else None)), defaultOp) {
  private val shuffle = deps.head.asInstanceOf[ShuffleDependency[K, V]]
  override def partitioner: Option[Partitioner] = Some(part)
  def numPartitions: Int = part.numPartitions
  def compute(split: Int): Iterator[(K, C)] = {
    val fetched = shuffle.fetch(split)
    val combined: Iterator[(K, C)] = agg match {
      case None                      => fetched.asInstanceOf[Iterator[(K, C)]]                                 // partitionBy, sortByKey: just move
      case Some(a) if mapSideCombine => combineCombinersByKey(fetched.asInstanceOf[Iterator[(K, C)]], a.mergeCombiners)   // merge the partial results
      case Some(a)                   => combineValuesByKey(fetched.asInstanceOf[Iterator[(K, V)]], a)          // all the work happens after the shuffle
    }
    keyOrdering.fold(combined)(ord => combined.toVector.sortBy(_._1)(using ord).iterator)                      // sortByKey: local sort per partition
  }
}

// groups two Pair RDDs by key (the basis of all joins). A parent that already has the right partitioner
// is read locally through a NARROW dependency: this is why co-partitioned joins need no shuffle.
class CoGroupedRDD[K, V, W](left: RDD[(K, V)], right: RDD[(K, W)], part: Partitioner)
    extends RDD[(K, (Iterable[V], Iterable[W]))](left.sc, List(CoGroupedRDD.dependency(left, part), CoGroupedRDD.dependency(right, part)), "cogroup") {
  override def partitioner: Option[Partitioner] = Some(part)
  def numPartitions: Int = part.numPartitions
  def compute(split: Int): Iterator[(K, (Iterable[V], Iterable[W]))] = {
    def read(dep: Dependency): Iterator[(K, Any)] = dep match {
      case s: ShuffleDependency[?, ?] => s.fetch(split).asInstanceOf[Iterator[(K, Any)]]
      case n                          => n.rdd.iterator(split).asInstanceOf[Iterator[(K, Any)]]
    }
    val groups = mutable.LinkedHashMap[K, (mutable.ArrayBuffer[V], mutable.ArrayBuffer[W])]()
    def group(k: K) = groups.getOrElseUpdate(k, (mutable.ArrayBuffer[V](), mutable.ArrayBuffer[W]()))
    read(deps(0)).foreach { case (k, v) => group(k)._1 += v.asInstanceOf[V] }
    read(deps(1)).foreach { case (k, w) => group(k)._2 += w.asInstanceOf[W] }
    groups.iterator.map { case (k, (vs, ws)) => (k, (new ValuesOfKey(vs.toVector), new ValuesOfKey(ws.toVector))) }
  }
}
object CoGroupedRDD {
  def dependency[K, V](rdd: RDD[(K, V)], part: Partitioner): Dependency =
    if (rdd.partitioner.contains(part)) new NarrowDependency(rdd)           // already partitioned like this: no shuffle
    else new ShuffleDependency[K, V](rdd, part, "cogroup", None)
}

// the Iterable[V] that groupByKey / cogroup hand out (prints like the book: Seq(20, 30); real Spark: CompactBuffer(20, 30))
final class ValuesOfKey[V](val elems: Vector[V]) extends Iterable[V] {
  def iterator: Iterator[V] = elems.iterator
  override protected def className: String = "Seq"
}

// which partitioner do key-based operations use when you do not pass one? An existing one if a parent has it
// (so that parent needs no shuffle), otherwise a HashPartitioner with as many partitions as the biggest parent.
def defaultPartitioner(rdds: RDD[?]*): Partitioner =
  rdds.flatMap(_.partitioner).sortBy(p => -p.numPartitions).headOption.getOrElse(new HashPartitioner(rdds.map(_.numPartitions).max))

// ----- Pair RDD operations: only available on RDD[(K, V)] (sections 10 - 14, 18) ------------------------
extension [K, V](self: RDD[(K, V)]) {
  def partitionBy(partitioner: Partitioner): RDD[(K, V)] =
    if (self.partitioner.contains(partitioner)) self else new ShuffledRDD[K, V, V](self, partitioner, "partitionBy")

  // the mother of all per-key aggregations
  def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C,
                      mapSideCombine: Boolean = true, op: String = "combineByKey"): RDD[(K, C)] = {
    val agg = Aggregator(createCombiner, mergeValue, mergeCombiners)
    val partitioner = defaultPartitioner(self)
    if (self.partitioner.contains(partitioner))        // keys are ALREADY co-located: aggregate inside each partition, no shuffle
      new MapPartitionsRDD[(K, C), (K, V)](self, op, (_, records) => combineValuesByKey(records, agg), preservesPartitioning = true)
    else new ShuffledRDD[K, V, C](self, partitioner, op, Some(agg), mapSideCombine)
  }
  def groupByKey(): RDD[(K, Iterable[V])] =            // NO map-side combine: grouping does not make the data smaller
    self.combineByKey[ValuesOfKey[V]](v => new ValuesOfKey(Vector(v)), (buf, v) => new ValuesOfKey(buf.elems :+ v),
      (b1, b2) => new ValuesOfKey(b1.elems ++ b2.elems), mapSideCombine = false, op = "groupByKey").asInstanceOf[RDD[(K, Iterable[V])]]
  def reduceByKey(func: (V, V) => V): RDD[(K, V)] = self.combineByKey[V](v => v, func, func, op = "reduceByKey")
  def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)] = self.combineByKey[V](v => func(zeroValue, v), func, func, op = "foldByKey")
  def aggregateByKey[U](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U): RDD[(K, U)] =
    self.combineByKey[U](v => seqOp(zeroValue, v), seqOp, combOp, op = "aggregateByKey")

  def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] = new CoGroupedRDD(self, other, defaultPartitioner(self, other))
  def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = self.sc.withCallSite("join") {
    self.cogroup(other).flatMapValues[(V, W)] { case (vs, ws) => for (v <- vs.iterator; w <- ws.iterator) yield (v, w) } }
  def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = self.sc.withCallSite("leftOuterJoin") {
    self.cogroup(other).flatMapValues[(V, Option[W])] { case (vs, ws) =>
      if (ws.isEmpty) vs.iterator.map(v => (v, None)) else for (v <- vs.iterator; w <- ws.iterator) yield (v, Some(w)) } }
  def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = self.sc.withCallSite("rightOuterJoin") {
    self.cogroup(other).flatMapValues[(Option[V], W)] { case (vs, ws) =>
      if (vs.isEmpty) ws.iterator.map(w => (None, w)) else for (v <- vs.iterator; w <- ws.iterator) yield (Some(v), w) } }
  def fullOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], Option[W]))] = self.sc.withCallSite("fullOuterJoin") {
    self.cogroup(other).flatMapValues[(Option[V], Option[W])] { case (vs, ws) =>
      if (ws.isEmpty) vs.iterator.map(v => (Some(v), None)) else if (vs.isEmpty) ws.iterator.map(w => (None, Some(w)))
      else for (v <- vs.iterator; w <- ws.iterator) yield (Some(v), Some(w)) } }

  def sortByKey(ascending: Boolean = true, numPartitions: Int = self.numPartitions)(using ord: Ordering[K]): RDD[(K, V)] =
    new ShuffledRDD[K, V, V](self, new RangePartitioner(numPartitions, self, ascending), "sortByKey", keyOrdering = Some(if (ascending) ord else ord.reverse))

  def mapValues[U](f: V => U): RDD[(K, U)] =           // the key cannot change -> safe to keep the partitioner
    new MapPartitionsRDD[(K, U), (K, V)](self, "mapValues", (_, it) => it.map { case (k, v) => (k, f(v)) }, preservesPartitioning = true)
  def flatMapValues[U](f: V => IterableOnce[U]): RDD[(K, U)] =
    new MapPartitionsRDD[(K, U), (K, V)](self, "flatMapValues", (_, it) => it.flatMap { case (k, v) => f(v).iterator.map(u => (k, u)) }, preservesPartitioning = true)
  def keys: RDD[K] = self.sc.withCallSite("keys") { self.map(_._1) }
  def values: RDD[V] = self.sc.withCallSite("values") { self.map(_._2) }

  // pair actions
  def countByKey(): Map[K, Long] = self.sc.withCallSite("countByKey") {
    val counted = self.mapValues(_ => 1L).reduceByKey(_ + _)
    self.sc.runJob(counted, "countByKey")(_.toVector).flatten.toMap }
  def collectAsMap(): Map[K, V] = self.sc.runJob(self, "collectAsMap")(_.toVector).flatten.toMap       // a later pair overwrites an earlier one
  def lookup(key: K): Seq[V] = self.partitioner match {
    case Some(p) => self.sc.runJobOn(self, "lookup", Seq(p.getPartition(key)))(_.filter(_._1 == key).map(_._2).toVector).flatten
    case None    => self.sc.runJob(self, "lookup")(_.filter(_._1 == key).map(_._2).toVector).flatten
  }
}

// ----- the SparkContext: entry point + scheduler + block manager + a fake cluster -----------------------
object TaskContext { var partitionId: Int = -1; var worker: Int = -1 }      // set while a task runs: "where am I?"

class SparkContext(val numWorkers: Int = 3, val defaultParallelism: Int = 4) {
  // a fake distributed file system (think HDFS): path -> lines
  val fs = mutable.LinkedHashMap[String, Vector[String]]()
  var blockSize: Int = 1000                               // LINES per block (HDFS: 128 MB). One block becomes one partition.
  def writeFile(path: String, lines: Seq[String]): Unit = fs(path) = lines.toVector

  // creating RDDs (section 6)
  def parallelize[T](seq: Seq[T], numPartitions: Int = defaultParallelism): RDD[T] = new ParallelCollectionRDD(this, seq.toIndexedSeq, numPartitions)
  def textFile(path: String, minPartitions: Int = 2): RDD[String] = new HadoopRDD(this, path, minPartitions)

  // bookkeeping
  private var rddIds = 0; private var shuffleIds = 0; private var jobIds = 0; private var stageIds = 0
  def newRddId(): Int = { rddIds += 1; rddIds - 1 }
  def newShuffleId(): Int = { shuffleIds += 1; shuffleIds - 1 }
  var callSite: Option[String] = None                     // lets distinct/join/... label their internal RDDs with their own name
  def withCallSite[A](name: String)(body: => A): A = if (callSite.isDefined) body else { callSite = Some(name); try body finally callSite = None }

  // switches and counters for the demos
  var logJobs = false; var trace = false
  private val counters = mutable.LinkedHashMap[String, Long]()
  List("jobs", "stages", "skipped stages", "tasks", "records shuffled", "lines read from file", "partitions read from cache").foreach(counters(_) = 0L)
  def inc(name: String, by: Long = 1L): Unit = counters(name) = counters(name) + by
  def measure[A](label: String)(body: => A): A = {
    val before = counters.toMap
    val result = body
    val work = counters.toList.map { case (name, value) => (name, value - before(name)) }.filter(_._2 != 0)
    println(s"   >> [$label] " + (if (work.isEmpty) "nothing happened on the cluster" else work.map { case (name, n) => s"$name=$n" }.mkString(", ")))
    result
  }
  def quietly[A](body: => A): A = {                       // run something without it showing up in counters / logs
    val saved = counters.toMap; val (savedLog, savedTrace) = (logJobs, trace); logJobs = false; trace = false
    try body finally { saved.foreach { case (name, value) => counters(name) = value }; logJobs = savedLog; trace = savedTrace }
  }
  private def log(message: => String): Unit = if (logJobs) println(s"   [driver] $message")

  // --- the scheduler (section 24): action -> job -> stages (cut at shuffles) -> tasks (one per partition) ---
  def runJob[T, U](rdd: RDD[T], action: String)(task: Iterator[T] => U): Vector[U] = runJobOn(rdd, action, 0 until rdd.numPartitions)(task)
  def runJobOn[T, U](rdd: RDD[T], action: String, partitions: Seq[Int])(task: Iterator[T] => U): Vector[U] = {
    val jobId = jobIds; jobIds += 1; inc("jobs")
    log(s"job $jobId: action '$action' on ${rdd.kind}[${rdd.id}]")
    submitMissingParentStages(rdd, mutable.Set[Int]())                    // first everything BEFORE the last shuffle ...
    val stageId = stageIds; stageIds += 1; inc("stages")
    log(s"  stage $stageId (ResultStage): ${partitions.size} task(s), pipeline ${pipeline(rdd)}")
    partitions.map(p => runTask(p) { task(rdd.iterator(p)) }).toVector    // ... then the stage that computes the action's result
  }
  // walk the lineage backwards; every shuffle dependency needs a stage that writes its shuffle files (unless they exist)
  private def submitMissingParentStages(rdd: RDD[?], visited: mutable.Set[Int]): Unit =
    if (visited.add(rdd.id) && !isFullyCached(rdd)) rdd.deps.foreach {     // a fully cached RDD cuts the lineage: nothing behind it is needed
      case s: ShuffleDependency[?, ?] =>
        val missing = (0 until s.rdd.numPartitions).filterNot(p => shuffleFiles.contains((s.shuffleId, p)))
        if (missing.isEmpty) { inc("skipped stages"); log(s"  stage skipped: the shuffle files of shuffle ${s.shuffleId} (${s.op}) still exist") }
        else {
          submitMissingParentStages(s.rdd, visited)
          s.partitioner.numPartitions                                      // (forces a RangePartitioner to sample on the driver)
          val stageId = stageIds; stageIds += 1; inc("stages")
          log(s"  stage $stageId (ShuffleMapStage, writes shuffle files for ${s.op}): ${missing.size} task(s), pipeline ${pipeline(s.rdd)}")
          missing.foreach(p => runTask(p) { s.runMapTask(p) })
        }
      case n => submitMissingParentStages(n.rdd, visited)
    }
  // a task = the work for ONE partition, executed on a worker. Whatever it prints is shown with a [worker-N] prefix.
  private def runTask[U](partition: Int)(body: => U): U = {
    inc("tasks")
    val worker = partition % numWorkers
    val (outerPartition, outerWorker) = (TaskContext.partitionId, TaskContext.worker)
    TaskContext.partitionId = partition; TaskContext.worker = worker
    val stdout = new java.io.ByteArrayOutputStream()
    try Console.withOut(stdout)(body)
    finally {
      TaskContext.partitionId = outerPartition; TaskContext.worker = outerWorker
      stdout.toString.linesIterator.foreach(line => println(s"      [worker-$worker] $line"))
    }
  }
  // all RDDs that are computed together in the stage that ends at `rdd` (connected by narrow dependencies)
  private def stageRdds(rdd: RDD[?]): (List[RDD[?]], List[ShuffleDependency[?, ?]]) = {
    val rdds = mutable.LinkedHashMap[Int, RDD[?]](); val shuffles = mutable.ListBuffer[ShuffleDependency[?, ?]]()
    def visit(r: RDD[?]): Unit = if (!rdds.contains(r.id)) { rdds(r.id) = r; r.deps.foreach { case s: ShuffleDependency[?, ?] => shuffles += s; case n => visit(n.rdd) } }
    visit(rdd)
    (rdds.values.toList.sortBy(_.id), shuffles.toList)
  }
  private def pipeline(rdd: RDD[?]): String = stageRdds(rdd)._1.map(r => s"${r.op}[${r.id}]").mkString(" -> ")
  def showStages(rdd: RDD[?]): Unit = {
    var next = 0
    def stage(last: RDD[?], kind: String): Int = {
      val (_, shuffles) = stageRdds(last)
      val parents = shuffles.map(s => stage(s.rdd, s"ShuffleMapStage (writes shuffle files for ${s.op})"))
      val id = next; next += 1
      println(s"      Stage $id - $kind: ${last.numPartitions} tasks (one per partition)")
      println(s"         pipelined in one pass: ${pipeline(last)}" + (if (parents.nonEmpty) s"      reads the shuffle output of stage ${parents.mkString(", ")}" else ""))
      id
    }
    stage(rdd, "ResultStage (computes the result of the action)")
  }

  // --- shuffle files: (shuffleId, map partition) -> one bucket of records per reduce partition. They live on the worker of the map task. ---
  val shuffleFiles = mutable.LinkedHashMap[(Int, Int), Array[Vector[(Any, Any)]]]()
  def showShuffleFiles(): Unit = shuffleFiles.foreach { case ((shuffleId, mapPartition), buckets) =>
    println(s"      shuffle $shuffleId, written by map task $mapPartition on worker-${mapPartition % numWorkers}: " +
      buckets.zipWithIndex.map { case (b, r) => s"for partition $r: ${b.mkString("[", " ", "]")}" }.mkString("  "))
  }

  // --- the cache (block manager): (rddId, partition) -> data + the workers that hold a copy ---
  private val cache = mutable.LinkedHashMap[(Int, Int), (Vector[Any], Set[Int])]()
  def cachedBlocks: Int = cache.size
  def isFullyCached(rdd: RDD[?]): Boolean = rdd.storageLevel != StorageLevel.NONE && (0 until rdd.numPartitions).forall(p => cache.contains((rdd.id, p)))
  def getOrCompute[T](rdd: RDD[T], split: Int): Iterator[T] = cache.get((rdd.id, split)) match {
    case Some((data, _)) => inc("partitions read from cache"); data.iterator.asInstanceOf[Iterator[T]]
    case None =>
      val data = rdd.computeTraced(split).toVector                         // the WHOLE partition is materialised
      cache((rdd.id, split)) = (data, (0 until rdd.storageLevel.replication).map(i => (split + i) % numWorkers).toSet)
      data.iterator
  }
  def dropBlocks(rddId: Int): Unit = cache.keys.filter(_._1 == rddId).toList.foreach(cache.remove)

  // --- failure injection (section 23): the worker loses its memory (cached partitions) and local disk (shuffle files) ---
  def crashWorker(worker: Int): Unit = {
    var lostBlocks = 0
    cache.toList.foreach { case (block, (data, holders)) =>
      if (holders.contains(worker)) { if (holders.size == 1) { cache.remove(block); lostBlocks += 1 } else cache(block) = (data, holders - worker) } }
    val lostFiles = shuffleFiles.keys.filter(_._2 % numWorkers == worker).toList
    lostFiles.foreach(shuffleFiles.remove)
    println(s"   !! worker-$worker crashed: $lostBlocks cached partition(s) without replica lost, ${lostFiles.size} shuffle file(s) lost (a fresh worker takes over)")
  }
}
