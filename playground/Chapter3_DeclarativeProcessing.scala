// =====================================================================================================
//  CHAPTER 3 - DECLARATIVE DATA PROCESSING                         study companion, runs in a browser
//  (Spark SQL: DataFrames, Catalyst, SQL, Datasets)
// =====================================================================================================
//  HOW TO USE
//    1. Open https://compilebytes.com/tools/scala , paste this whole file into Main.scala, press Run.
//    2. Read the code of a section, predict what it prints, then compare with the real output.
//    3. Too much output? Put section numbers in ONLY (just below), e.g. Set(15, 16), and run again.
//
//  WHAT IS THIS
//    The playground has no Spark library. So PART 2 (bottom half of this file) is a "mini Spark SQL":
//    DataFrames with a schema, Column expressions, a logical plan, an ANALYZER, a small CATALYST
//    optimizer (filter pushdown, column pruning, partition pruning, cross join -> inner join), a
//    physical planner with a cost-based join choice, a SQL parser and typed Datasets.
//    The code in PART 1 is therefore (almost) exactly what you would write against real Spark.
//    Two unavoidable differences, both Scala 3 vs the course's Scala 2.13:
//        import spark.implicits.*        (Scala 2.13: import spark.implicits._)
//        import functions.*              (real Spark: import org.apache.spark.sql.functions._)
//
//  EXTRAS that real Spark does not have (they make invisible things visible):
//      spark.measure("label") { ... }   prints how much work the engine did inside the block
//      spark.optimizerEnabled = false   switch Catalyst's optimizer off and feel the difference
//      df.explain(true)                 real Spark has this too: analyzed / optimized / physical plan
//      compiles("some code")            asks the Scala COMPILER whether a piece of code type-checks.
//                                       That is how this file shows "compile time vs run time" errors.
//  The times printed by this toy engine mean little. Look at the COUNTS (rows, values, pairs).
// =====================================================================================================
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.deriving.Mirror
import scala.compiletime.{constValueTuple, summonAll}
import functions.*

val ONLY: Set[Int] = Set()          // empty = run every section.   Example: Set(15, 16)

@main def main(): Unit = {
  s01_imperativeVsDeclarative()     // the "how" of RDDs vs the "what" of DataFrames
  s02_motivationalExample()         // scholarship candidates: three RDD solutions
  s03_structure()                   // unstructured / semi-structured / structured; opaque lambdas
  s04_sparkSql()                    // goals + architecture: API -> Catalyst -> RDDs
  s05_dataFrames()                  // rows + schema, dynamically typed
  s06_creatingFromRdd()             // toDF, toDF(names), case class, manual schema
  s07_schemaTypes()                 // Spark SQL types, nested types
  s08_creatingFromFiles()           // spark.read.json with inferred schema
  s09_firstExample()                // select - where - orderBy - show
  s10_selectionAndColumns()         // select, Column objects, unbound vs bound columns
  s11_filteringAndSorting()         // where / filter, orderBy / sort
  s12_groupingAndAggregation()      // groupBy, RelationalGroupedDataset, agg
  s13_joins()                       // join types, cross join + filter
  s14_actions()                     // show printSchema collect count first take foreach reduce
  s15_motivationalExampleRevisited()// the scholarship query with DataFrames
  s16_catalyst()                    // the pipeline and the optimizations, one by one
  s17_tungsten()                    // what Tungsten adds, cost of mixing RDDs and DataFrames
  s18_sql()                         // spark.sql, temp views, dialect, syntax tour
  s19_limitations()                 // not statically typed, needs structure
  s20_datasetsMotivation()          // Row = Any everywhere; DataFrame = Dataset[Row]
  s21_creatingDatasets()            // toDS, as[T]
  s22_datasetTransformations()      // typed and untyped transformations, black-box lambdas
  s23_datasetGrouping()             // groupByKey, agg, mapGroups, flatMapGroups, reduceGroups
  s24_motivationalExampleWithDatasets()
  s25_rddVsDataFrameVsDataset()     // when to use which
  s26_exercises()                   // the chapter's exercises on small data
  println("\nDone. Tip: set ONLY = Set(n) at the top to replay a single section.")
}

def section(n: Int, title: String)(body: => Unit): Unit =
  if (ONLY.isEmpty || ONLY.contains(n)) {
    println(); println("=" * 100); println(s"SECTION $n - $title"); println("=" * 100)
    body
  }
def sub(title: String): Unit = println(s"\n--- $title ---")
// does this piece of Scala code type-check? Answered by the compiler, at COMPILE time.
inline def compiles(inline code: String): Boolean = scala.compiletime.testing.typeChecks(code)
inline def compileError(inline code: String): String = scala.compiletime.testing.typeCheckErrors(code).headOption.fold("(it compiles)")(_.message)
// runs a block and prints the exception it throws: errors that only show up at RUN time
def atRuntime(label: String)(body: => Any): Unit =
  try { body; println(s"   $label: no error") }
  catch { case e: Exception => println(s"   $label\n      -> ${e.getClass.getSimpleName}: ${e.getMessage}") }

// the sales data used by most examples of the chapter
val salesData = List(
  ("Electronics", "Brussels", 1300.0),
  ("Books", "Ghent", 30.0),
  ("Groceries", "Brussels", 45.0),
  ("Electronics", "Antwerp", 2000.0),
  ("Books", "Brussels", 70.0)
)
case class Sale(category: String, store: String, amount: Double)

// =====================================================================================================
//  PART 1 - THE CHAPTER, SECTION BY SECTION
// =====================================================================================================

// -----------------------------------------------------------------------------------------------------
// 1. IMPERATIVE vs DECLARATIVE
//    RDD API = imperative: you write the exact steps (the HOW). You are the optimizer: reduceByKey or
//    groupByKey? filter before or after the join? what to persist?
//    DataFrames / Datasets / SQL = declarative: you describe the result (the WHAT). The query is a
//    logical plan; the Catalyst optimizer decides how to compute it.
//    Three advantages: (1) performance through optimization (predicate pushdown, column pruning, join
//    reordering), (2) simplicity and accessibility (also for SQL analysts), (3) future-proofing (the same
//    query gets faster when Spark's optimizer gets smarter).
// -----------------------------------------------------------------------------------------------------
def s01_imperativeVsDeclarative(): Unit = section(1, "Imperative (how) vs declarative (what)") {
  val spark = new SparkSession(); import spark.implicits.*
  val sc = spark.sparkContext
  val storeAmounts = salesData.map { case (_, store, amount) => (store, amount) }

  sub("imperative, RDD: YOU choose the steps, Spark executes them literally")
  val rdd = sc.parallelize(Seq.fill(1000)(storeAmounts).flatten)                     // 5000 (store, amount) pairs
  spark.measure("groupByKey().mapValues(_.sum)   your choice: slow") { rdd.groupByKey().mapValues(_.sum).collect() }
  spark.measure("reduceByKey(_ + _)              your choice: fast") { rdd.reduceByKey(_ + _).collect() }

  sub("declarative, DataFrame: you say WHAT you want; Catalyst picks the plan")
  val totals = Seq.fill(1000)(storeAmounts).flatten.toDF("store", "amount").groupBy("store").sum("amount")
  totals.show()
  totals.explain()
  println("   ^ you wrote 'group, then sum' and still got a partial aggregation BEFORE the shuffle (= map-side combine).")
}

// -----------------------------------------------------------------------------------------------------
// 2. MOTIVATIONAL EXAMPLE: selecting scholarship candidates, with RDDs
//    Count the people who are from Belgium, have debt, and have financial dependents.
//    The data is split over two datasets that share an id.
// -----------------------------------------------------------------------------------------------------
case class Demographics(
  id: Int,
  age: Int,
  codingBootcamp: Boolean,
  country: String,
  gender: String,
  isEthnicMinority: Boolean,
  servedMilitary: Boolean)

case class Finances(
  id: Int,
  hasDebt: Boolean,
  hasFinancialDependents: Boolean,
  hasStudentLoans: Boolean,
  income: Int)

// made-up people: every 10th is from Belgium, every 2nd has debt, every 3rd has dependents -> 1 in 30 qualifies
def demographicsData(n: Int): Seq[Demographics] =
  (0 until n).map(i => Demographics(i, 18 + i % 40, i % 7 == 0, if (i % 10 == 0) "Belgium" else "Elsewhere", if (i % 2 == 0) "F" else "M", i % 5 == 0, i % 11 == 0))
def financesData(n: Int): Seq[Finances] =
  (0 until n).map(i => Finances(i, i % 2 == 0, i % 3 == 0, i % 4 == 0, 1000 + i % 3000))

def s02_motivationalExample(): Unit = section(2, "Motivational example: scholarship candidates with RDDs") {
  val spark = new SparkSession()
  val sc = spark.sparkContext
  val n = 3000
  val demographics: RDD[(Int, Demographics)] = sc.parallelize(demographicsData(n)).map(d => (d.id, d)).persist()
  val finances: RDD[(Int, Finances)] = sc.parallelize(financesData(n)).map(f => (f.id, f)).persist()
  println(s"   $n people in demographics, $n records in finances")

  sub("Solution 1: join and filter     - the natural way to write it")
  val result1 = spark.measure("solution 1") {
    demographics.join(finances) // type is RDD[(Int, (Demographics, Finances))]
      .filter { case (id, (demographics, finances)) =>
        demographics.country == "Belgium" &&
        finances.hasDebt &&
        finances.hasFinancialDependents
      }.count()
  }

  sub("Solution 2: filter, filter, and join     - 'filter early', the developer is the optimizer")
  val result2 = spark.measure("solution 2") {
    val filteredFinances: RDD[(Int, Finances)] = finances.filter { case (id, fin) =>
      fin.hasDebt && fin.hasFinancialDependents
    }
    demographics.filter { case (id, demo) =>
      demo.country == "Belgium"
    }.join(filteredFinances).count()
  }

  sub("Solution 3: cartesian, filter, filter     - correct, and catastrophically slow")
  val result3 = spark.measure("solution 3") {
    val cartesian: RDD[((Int, Demographics), (Int, Finances))] = demographics.cartesian(finances)
    cartesian.filter {
      case (p1, p2) => p1._1 == p2._1 // manually perform the join
    }.filter {
      case (p1, p2) =>
        p1._2.country == "Belgium" &&
        p2._2.hasDebt &&
        p2._2.hasFinancialDependents
    }.count()
  }
  println(s"\n   all three find the same $result1 = $result2 = $result3 candidates, but look at the work:")
  println("   1: both datasets are shuffled COMPLETELY, most joined records are thrown away afterwards")
  println("   2: only the records that survive the filters are shuffled (the book: 3.6x faster than 1)")
  println(s"   3: $n x $n = ${n.toLong * n} pairs are built first (the book: 177x slower than 1). With 1M x 1M: 10^12 pairs.")
  println("   What we WANT: write it like solution 1, get the speed of solution 2. That needs an optimizer,")
  println("   and an optimizer needs to understand the data (schema) and the operations (no black-box lambdas).")
}

// -----------------------------------------------------------------------------------------------------
// 3. UNSTRUCTURED vs STRUCTURED DATA
//    unstructured    : no data model at all (plain text, images). The system only sees a blob.
//    semi-structured : no rigid schema, but self-describing markers/tags (JSON, XML, log files).
//    structured      : rigid, predefined schema, defined OUTSIDE the data (a database table).
//    An RDD treats its elements as opaque objects and its functions as black boxes -> no optimization.
// -----------------------------------------------------------------------------------------------------
def s03_structure(): Unit = section(3, "Unstructured vs structured data; why an RDD cannot be optimized") {
  println("""   unstructured   : "Alice has 1500 euro on her account, which is not considered risky"
   semi-structured: {"name": "Alice", "balance": 1500.0}   {"name": "Bob", "balance": 20.0, "risk": true}
   structured     : | name: string | balance: double | risk: boolean |   <- schema, enforced for every row
                    | Alice        | 1500.0          | false         |""")

  sub("an RDD of case class objects: structured for the Scala COMPILER, opaque for SPARK")
  case class Account(name: String, balance: Double, risk: Boolean)
  val spark = new SparkSession(); import spark.implicits.*
  val accounts: RDD[Account] = spark.sparkContext.parallelize(Seq(Account("Alice", 1500.0, false), Account("Bob", 20.0, true)))
  val predicate = (account: Account) => account.balance > 1000
  println("   accounts.filter(account => account.balance > 1000)  -> " + accounts.filter(predicate).collect().mkString(", "))
  println("   what Spark sees of that lambda: " + predicate)
  println("   ^ compiled code. Spark can CALL it, but cannot look inside: it does not know that only `balance` is used.")
  println("     So: no column pruning (must load whole Account objects), no moving the filter around.")

  sub("a Column expression is DATA: a tree that Spark can inspect, rewrite and optimize")
  val condition = $"balance" > 1000
  println("   $\"balance\" > 1000   prints as: " + condition + "     and is the tree: " + condition.expr)
  println("   This is the 'database view' of data + operations that Spark SQL provides:")
  println("   like WHERE balance > 1000 on a table with a schema, where the optimizer can plan (e.g. use an index).")
}

// -----------------------------------------------------------------------------------------------------
// 4. SPARK SQL
//    Goals: (1) a unified relational API: DataFrames + SQL, (2) high performance through database-style
//    optimization: Catalyst (plans) + Tungsten (execution), (3) one interface for many data sources.
//    Architecture:
//       API        : DataFrames / Datasets / SQL strings  -> you build a declarative LOGICAL PLAN
//       Optimizer  : Catalyst rewrites it into the most efficient PHYSICAL PLAN it can find
//       Execution  : everything ends up as a DAG of RDDs on the same core engine (with Tungsten bytecode)
//       Entry points: programs, spark-shell / spark-sql, BI tools over JDBC
// -----------------------------------------------------------------------------------------------------
def s04_sparkSql(): Unit = section(4, "Spark SQL: architecture, mixing DataFrames and RDDs") {
  val spark = new SparkSession(); import spark.implicits.*
  val salesDf = salesData.toDF("category", "store", "amount")

  sub("the API builds a plan, it does not run anything (same laziness as RDD transformations)")
  val query = spark.measure("select + where") { salesDf.select($"category", $"amount").where($"amount" > 100) }
  println("   the plan that was built:")
  query.explain(true)

  sub("same engine underneath: you can go DataFrame -> RDD -> DataFrame (at a cost, see section 17)")
  val rdd: RDD[Row] = salesDf.rdd                                             // .rdd: leave the optimized world
  val shouted: RDD[(String, Double)] = rdd.map(row => (row.getString(0).toUpperCase, row.getDouble(2)))   // arbitrary RDD code
  val back = shouted.toDF("category", "amount")                               // .toDF(): back to DataFrames
  back.show()
}

// -----------------------------------------------------------------------------------------------------
// 5. DATAFRAMES
//    A DataFrame = a table: rows and NAMED columns. Technically an RDD of Row objects + a schema.
//    The schema (column names + Spark SQL types) is what enables optimization.
//    'Untyped' really means: no STATIC type for the rows. A DataFrame is dynamically typed: the schema is
//    known and checked at run time, not by the Scala compiler.
// -----------------------------------------------------------------------------------------------------
def s05_dataFrames(): Unit = section(5, "DataFrames: rows + schema, dynamically typed") {
  val spark = new SparkSession(); import spark.implicits.*
  val salesDf = salesData.toDF("category", "store", "amount")
  salesDf.printSchema()
  salesDf.show()
  val first: Row = salesDf.first()
  println(s"   one record is a generic Row: $first   (to the compiler just a bag of ${first.length} values of type Any)")

  sub("the compiler does not know the schema: a misspelled column COMPILES ...")
  println("   does `salesDf.select($\"categorie\")` compile? " + compiles("""salesDf.select($"categorie")"""))
  sub("... and fails at RUN time, when Catalyst analyzes the plan against the schema")
  atRuntime("""salesDf.select($"categorie")""") { salesDf.select($"categorie") }
  println("   (the book: 'when an action is called'. Real Spark usually already analyzes when the transformation")
  println("    is called, like this mini engine. The point is the same: you only find out when the program RUNS.)")
}

// -----------------------------------------------------------------------------------------------------
// 6. CREATING DATAFRAMES FROM AN EXISTING RDD
//    (a) with an INFERRED schema: toDF() on tuples, toDF(names), or - best - an RDD of a case class
//    (b) with a MANUAL schema: RDD[Row] + StructType, glued together by spark.createDataFrame
// -----------------------------------------------------------------------------------------------------
def s06_creatingFromRdd(): Unit = section(6, "Creating DataFrames from an RDD: inferred and manual schema") {
  val spark = new SparkSession(); import spark.implicits.*
  val sc = spark.sparkContext
  val salesRdd: RDD[(String, String, Double)] = sc.parallelize(salesData)

  sub("df: RDD of tuples, no names given -> types are inferred, names are positional: _1, _2, _3")
  val df = salesRdd.toDF()
  df.printSchema()

  sub("dfNames: you pass the column names, Spark still infers the types")
  val dfNames = salesRdd.toDF("category", "store", "amount")
  dfNames.printSchema()

  sub("dfRef: RDD of a case class -> names AND types come from the case class (reflection). Preferred.")
  val salesRddCase: RDD[Sale] =
    salesRdd.map { case (category, store, amount) => Sale(category, store, amount) }
  val dfRef = salesRddCase.toDF()
  dfRef.printSchema()
  println("   note `string`, `double`: these are Spark SQL types (used by Catalyst at run time),")
  println("   not the Scala types String, Double (used by the compiler at compile time).")

  sub("manual schema: for raw sources (CSV, text) where the schema is not in the data")
  spark.fs.write("/workspace/data/dataset/salesData.csv", salesData.map { case (c, s, a) => s"$c,$s,$a" })
  val schema = StructType(Seq(                                  // StructType = the whole schema = a collection of ...
    StructField("category", StringType, nullable = true),     // ... StructFields: one column = name + type + nullable
    StructField("store", StringType, nullable = true),
    StructField("amount", DoubleType, nullable = false)
  ))
  // step 1: parse the raw lines into generic Row objects (order and types must match the schema!)
  val rowRdd: RDD[Row] = sc.textFile("/workspace/data/dataset/salesData.csv").map {
    case line =>
      val attributes = line.split(",")
      Row(attributes(0), attributes(1), attributes(2).toDouble)
  }
  // step 2: bind the RDD[Row] (the data) to the StructType (the metadata)
  val salesDf = spark.createDataFrame(rowRdd, schema)
  salesDf.printSchema()
  salesDf.show()
}

// -----------------------------------------------------------------------------------------------------
// 7. DATAFRAME SCHEMAS: the restricted set of Spark SQL types
// -----------------------------------------------------------------------------------------------------
case class Account(
  balance: Double,
  employees: Array[Employee])

case class Employee(
  id: Int,
  name: String,
  jobTitle: String)

case class Project(
  title: String,
  team: Array[Employee],
  acct: Account)

def s07_schemaTypes(): Unit = section(7, "Schema types: primitive and complex (nested) types") {
  println("""   Scala type            Spark SQL type     details
   Byte                  ByteType           1 byte signed integers
   Short                 ShortType          2 byte signed integers
   Int                   IntegerType        4 byte signed integers
   Long                  LongType           8 byte signed integers
   java.math.BigDecimal  DecimalType        arbitrary precision signed decimals
   Float                 FloatType          4 byte floating point number
   Double                DoubleType         8 byte floating point number
   Array[Byte]           BinaryType         byte sequence values
   Boolean               BooleanType        true / false
   java.sql.Timestamp    TimestampType      date with y, m, d, h, m, s
   java.sql.Date         DateType           date with y, m, d
   String                StringType         character string values (UTF8)
   ---- complex types ----
   Array[T]              ArrayType(elementType, containsNull)
   Map[K, V]             MapType(keyType, valueType, valueContainsNull)
   case class            StructType(List[StructField])
   (this mini engine implements String, Int, Long, Double, Boolean + the three complex types)""")

  sub("complex types nest arbitrarily: the schema that Spark derives for the case class Project")
  val projectSchema: StructType = summon[Encoder[Project]].schema
  println("   as a value: " + projectSchema)
  println("   as a tree (printSchema):")
  projectSchema.printTreeString()
  println("   writing that StructType by hand is possible but verbose and error-prone. Let Spark infer it from")
  println("   a case class, or from a self-describing source (JSON, Parquet) - next section.")

  sub("a map column")
  val spark = new SparkSession(); import spark.implicits.*
  Seq(("alice", Map("math" -> 16, "physics" -> 13))).toDF("student", "grades").printSchema()
}

// -----------------------------------------------------------------------------------------------------
// 8. CREATING DATAFRAMES FROM EXTERNAL (SEMI-)STRUCTURED DATA
//    spark.read is the usual way to load data: JSON, CSV, Parquet / ORC (binary, columnar), JDBC, ...
//    Semi-structured and self-describing formats let Spark infer the schema from the data itself.
// -----------------------------------------------------------------------------------------------------
def s08_creatingFromFiles(): Unit = section(8, "Creating DataFrames from files: spark.read.json with an inferred schema") {
  val spark = new SparkSession()
  spark.fs.write("/workspace/data/dataset/salesData.json", Seq(
    """{"category": "Electronics", "store": "Brussels", "amount": 1300.0}""",
    """{"category": "Books", "store": "Ghent", "amount": 30.0, "customer_id": "c123"}""",
    """{"category": "Groceries", "store": "Brussels", "amount": 45.0}""",
    """{"category": "Electronics", "store": "Antwerp", "amount": 2000.0, "customer_id": "c456"}""",
    """{"category": "Books", "store": "Brussels", "amount": 70.0}"""))

  val salesDf = spark.read.json("/workspace/data/dataset/salesData.json")
  salesDf.printSchema()
  salesDf.show()
  println("   Spark scanned the file, found all four fields (customer_id is missing in some records) and their")
  println("   types, and filled the gaps with NULL: every row has the same structure.")

  sub("CSV has no types inside: without a schema every column is a string named _c0, _c1, ...")
  spark.fs.write("/workspace/data/dataset/salesData.csv", salesData.map { case (c, s, a) => s"$c,$s,$a" })
  spark.read.csv("/workspace/data/dataset/salesData.csv").printSchema()
  sub("... so you pass one (as a StructType, or as a DDL string)")
  spark.read.schema("category STRING, store STRING, amount DOUBLE").csv("/workspace/data/dataset/salesData.csv").printSchema()
}

// -----------------------------------------------------------------------------------------------------
// 9. OPERATIONS ON DATAFRAMES: a first example
//    Same execution model as RDDs: transformations are lazy, actions execute. But the operations are
//    declarative and far more high-level.
// -----------------------------------------------------------------------------------------------------
def s09_firstExample(): Unit = section(9, "A first example: select - where - orderBy - show") {
  val spark = new SparkSession(); import spark.implicits.*

  // equivalent to SQL:  SELECT category, amount FROM sales WHERE amount > 100 ORDER BY amount DESC
  val salesDf = salesData.toDF("category", "store", "amount")
                          .select($"category", $"amount")      // projection: keep 2 of the 3 columns
                          .where($"amount" > 100)              // keep the rows with amount > 100
                          .orderBy($"amount".desc)             // sort, highest amount first

  salesDf.show()                                               // the ACTION: only now the chain is executed
}

// -----------------------------------------------------------------------------------------------------
// 10. TRANSFORMATIONS: SELECTION, and how to refer to columns
//     def select(col: String, cols: String*): DataFrame      by name: simple and convenient
//     def select(cols: Column*): DataFrame                   by Column object: expressions, functions, aliases
// -----------------------------------------------------------------------------------------------------
def s10_selectionAndColumns(): Unit = section(10, "Selection; Column objects: unbound ($\"x\") vs bound (df(\"x\"))") {
  val spark = new SparkSession(); import spark.implicits.*
  val salesDf = salesData.toDF("category", "store", "amount")

  sub("select by String")
  salesDf.select("category", "store").show(2)
  sub("select by Column: calculations, functions, renaming")
  salesDf.select($"category", lower($"store"), ($"amount" * 2).as("double_amount")).show(2)

  sub("an UNBOUND column ($-notation) is just a name; it is resolved against whatever DataFrame it is used on")
  val roundedWithVAT = round($"amount" * 0.21)
  val smallSales = List(
    ("Electronics", 1300.0),
    ("Books", 30.0),
    ("Groceries", 45.0)
  ).toDF("category", "amount")
  // completely unrelated data
  val equipmentData = List(
    ("Linux server", 67382.23),
    ("Server rack", 23462.99),
    ("Cables", 9834.63)
  )
  val equipmentDf = equipmentData.toDF("name", "amount")
  smallSales.select(roundedWithVAT).show();
  equipmentDf.select(roundedWithVAT).show();

  sub("in a join an unbound name can be AMBIGUOUS; a BOUND column df(\"x\") remembers its DataFrame")
  val df1 = Seq((1, "left-1"), (2, "left-2")).toDF("id", "l")
  val df2 = Seq((1, "right-1"), (3, "right-3")).toDF("id", "r")
  atRuntime("""df1.join(df2, $"id" === $"id")""") { df1.join(df2, $"id" === $"id") }
  println("   df1.join(df2, df1(\"id\") === df2(\"id\")) works:")
  df1.join(df2, df1("id") === df2("id")).show()
  println(s"   unbound: ${($"id").expr}      bound: ${df1("id").expr}")

  sub("both kinds are just `Column` for the compiler: it cannot check names or types")
  val textAmounts = Seq(("Electronics", "a lot"), ("Books", "30")).toDF("category", "amount")     // amount holds STRINGS
  println("   does `textAmounts(\"amount\") > 100` compile? " + compiles("""textAmounts("amount") > 100"""))
  val suspicious = textAmounts.where(textAmounts("amount") > 100)
  atRuntime("suspicious.show()   <- the error only appears when the action runs") { suspicious.show() }
}

// -----------------------------------------------------------------------------------------------------
// 11. FILTERING AND SORTING
//     def where(conditionExpr: String): DataFrame     def where(condition: Column): DataFrame
//     where is an alias of filter.
//     def orderBy(sortCol: String, sortCols: String*): DataFrame     def orderBy(sortExprs: Column*): DataFrame
//     orderBy is an alias of sort. It is a WIDE transformation: a global sort of the whole DataFrame.
// -----------------------------------------------------------------------------------------------------
def s11_filteringAndSorting(): Unit = section(11, "Filtering (where = filter) and sorting (orderBy = sort)") {
  val spark = new SparkSession(); import spark.implicits.*
  val salesDf = salesData.toDF("category", "store", "amount")

  sub("four ways to write the same filter")
  val viaColumn  = salesDf.where($"store" === "Brussels" && $"amount" > 50).collect().toList
  val viaString  = salesDf.where("store = 'Brussels' AND amount > 50").collect().toList
  val viaFilter  = salesDf.filter($"store" === "Brussels" && $"amount" > 50).collect().toList
  val viaChained = salesDf.filter($"store" === "Brussels").filter($"amount" > 50).collect().toList
  println(s"   $viaColumn\n   all equal: ${viaColumn == viaString && viaString == viaFilter && viaFilter == viaChained}")
  println("   careful: columns are compared with === and =!= (== would compare the two Column OBJECTS)")

  sub("sort on two columns: category ascending, then amount descending")
  val sortedDf = salesDf.orderBy($"category".asc, $"amount".desc).show()
  println("   sort is the same thing: " + (salesDf.sort($"category".asc, $"amount".desc).collect().toList == salesDf.orderBy($"category".asc, $"amount".desc).collect().toList))
  salesDf.orderBy("store", "category").show()
}

// -----------------------------------------------------------------------------------------------------
// 12. GROUPING AND AGGREGATION
//     def groupBy(cols: Column*): RelationalGroupedDataset
//     groupBy does NOT return a DataFrame but a 'waiting' object: a grouping definition without results.
//     Like SQL: GROUP BY alone is not a query, it needs an aggregate function. Only count / sum / max /
//     min / mean / avg / agg(...) turn it into a DataFrame again.
// -----------------------------------------------------------------------------------------------------
def s12_groupingAndAggregation(): Unit = section(12, "Grouping and aggregation: groupBy, RelationalGroupedDataset, agg") {
  val spark = new SparkSession(); import spark.implicits.*
  val salesDf = salesData.toDF("category", "store", "amount")

  val groupedData = salesDf.groupBy("store")
  println("   salesDf.groupBy(\"store\") is a " + groupedData.getClass.getSimpleName + ", not a DataFrame:")
  println("   does `groupedData.filter($\"amount\" > 1)` compile? " + compiles("""groupedData.filter($"amount" > 1)"""))
  println("   does `groupedData.show()` compile?               " + compiles("""groupedData.show()"""))

  sub("the convenience shortcuts: one aggregation each")
  groupedData.count().show()
  groupedData.sum("amount").show()
  println("   max / min / avg (= mean): " + groupedData.max("amount").collect().toList + " / " + groupedData.min("amount").collect().toList + " / " + groupedData.avg("amount").collect().toList)

  sub("def agg(expr: Column, exprs: Column*): DataFrame     several aggregations on the same groups")
  val summaryDf = groupedData.agg(
    sum("amount").as("total_sales"),
    round(avg("amount")).as("average_sales"),
    count("*").as("number_of_sales")
  ).show()

  sub("performance: groupBy vs the RDD's groupByKey")
  println("   RDD: salesRdd.groupByKey().mapValues(_.sum) ships every record and builds a collection per key.")
  println("   You had to know that salesRdd.reduceByKey(_ + _) is better (map-side combine).")
  println("   DataFrame: write the natural salesDf.groupBy($\"store\").sum(\"amount\") and Catalyst plans this:")
  salesDf.groupBy($"store").sum("amount").explain()
}

// -----------------------------------------------------------------------------------------------------
// 13. JOINS
//     def join(right: DataFrame, joinExprs: Column, joinType: String): DataFrame
//     DataFrames are tables, not key-value pairs: you must SAY which columns to join on.
//     joinType: "inner" (default), "left_outer", "right_outer", "full_outer", "cross", ...
// -----------------------------------------------------------------------------------------------------
def s13_joins(): Unit = section(13, "Joins") {
  val spark = new SparkSession(); import spark.implicits.*
  val totalsDf   = Seq(("Books", 100.0), ("Electronics", 3300.0), ("Groceries", 45.0)).toDF("category", "total")
  val managersDf = Seq(("Electronics", "Alice"), ("Books", "Bob"), ("Apparel", "Charlie")).toDF("category", "manager")
  val onCategory = totalsDf("category") === managersDf("category")

  sub("inner (the default): only categories present on both sides")
  totalsDf.join(managersDf, onCategory).show()
  sub("left_outer: all rows of the left side, NULL where the right side has no match")
  totalsDf.join(managersDf, onCategory, "left_outer").show()
  sub("right_outer")
  totalsDf.join(managersDf, onCategory, "right_outer").show()
  sub("no join expression = cross join = cartesian product: 3 x 3 = 9 rows")
  println("   " + totalsDf.join(managersDf).count() + " rows")

  sub("cross join + where: the terrible RDD 'solution 3' ... but Catalyst rewrites it into an inner join")
  val crossThenFilter = totalsDf.join(managersDf).where(totalsDf("category") === managersDf("category"))
  crossThenFilter.explain(true)
  println("   ^ compare the analyzed plan (Join Cross + Filter) with the optimized one (Join Inner with a condition):")
  println("     the Cartesian product is never built.")
}

// -----------------------------------------------------------------------------------------------------
// 14. DATAFRAME ACTIONS: trigger the execution of the whole logical plan
// -----------------------------------------------------------------------------------------------------
def s14_actions(): Unit = section(14, "DataFrame actions") {
  val spark = new SparkSession(); import spark.implicits.*
  val salesDf = salesData.toDF("category", "store", "amount")

  sub("def show(): Unit / def show(numRows: Int): Unit     top rows (default 20) as a table; long strings are cut at 20 characters")
  salesDf.show(2)
  Seq(("a string of more than twenty characters", 1)).toDF("text", "n").show()
  sub("def printSchema: Unit")
  salesDf.printSchema()
  val all: Array[Row] = salesDf.collect()                      // def collect(): Array[Row]
  println("   collect()  = " + all.mkString(" "))
  println("   count()    = " + salesDf.count())                // def count(): Long
  println("   first()    = " + salesDf.first())                // def first(): Row
  println("   take(2)    = " + salesDf.take(2).mkString(" "))  // def take(n: Int): Array[Row]
  print("   foreach    : "); salesDf.foreach(row => print(row.getString(0) + " ")); println()    // def foreach(f: Row => Unit): Unit
  val biggest: Row = salesDf.reduce((r1, r2) => if (r1.getDouble(2) >= r2.getDouble(2)) r1 else r2)   // def reduce(func: (Row, Row) => Row): Row
  println("   reduce     = " + biggest + "   (the row with the largest amount)")
  println("   show is the equivalent of take followed by printing.")
}

// -----------------------------------------------------------------------------------------------------
// 15. BACK TO THE MOTIVATIONAL EXAMPLE, now with DataFrames
// -----------------------------------------------------------------------------------------------------
def s15_motivationalExampleRevisited(): Unit = section(15, "Motivational example revisited: natural style, optimized execution") {
  val spark = new SparkSession(); import spark.implicits.*
  val n = 3000
  val demographicsDf = demographicsData(n).toDF()
  val financesDf = financesData(n).toDF()

  // written like RDD solution 1: join first, then filter
  def query = demographicsDf.join(financesDf, demographicsDf("id") === financesDf("id"))
                .filter($"hasDebt" && $"hasFinancialDependents")
                .filter($"country" === "Belgium")

  val result = spark.measure("DataFrame query, as written above") { query.count() }
  println(s"   $result candidates")

  sub("what Catalyst did with it")
  query.explain(true)
  println("""   read the OPTIMIZED plan bottom-up and compare with the analyzed plan:
     - both Filters moved BELOW the join (filter early = the manual optimization of RDD solution 2)
     - of demographics only id + country are read, of finances only id + hasDebt + hasFinancialDependents
       (column pruning: something RDD solution 2 cannot do, it always loads whole objects)""")

  sub("the same query with the optimizer switched off: executed literally, like RDD solution 1")
  spark.optimizerEnabled = false
  spark.measure("optimizer OFF") { query.count() }
  spark.optimizerEnabled = true
  spark.measure("optimizer ON ") { query.count() }
  println("   We specified WHAT we wanted (a count of joined, filtered records), not HOW. In real Spark this version")
  println("   beats even the hand-optimized RDD solution 2, because of column pruning and Tungsten's generated code.")
}

// -----------------------------------------------------------------------------------------------------
// 16. CATALYST
//     A DataFrame query is not a black box but a logical plan (a tree, like an AST) that Catalyst can
//     analyze and rewrite. The Spark SQL pipeline:
//        SQL / DataFrame -> unresolved logical plan --analysis (catalog)--> logical plan
//        --logical optimization--> optimized logical plan --physical planning--> physical plans
//        --cost model--> selected physical plan --code generation--> RDDs
//     Optimizations: filter early (predicate pushdown), column pruning, partition pruning.
// -----------------------------------------------------------------------------------------------------
def s16_catalyst(): Unit = section(16, "Catalyst: the pipeline and its optimizations") {
  val spark = new SparkSession(); import spark.implicits.*
  val people = (1 to 1000).map(i => (s"person$i", 15 + i % 50, if (i % 4 == 0) "Ghent" else "Brussels", i * 10.0))
  val peopleDf = people.toDF("name", "age", "city", "salary")
  peopleDf.createOrReplaceTempView("people")

  sub("the whole pipeline for one query")
  spark.explainPipeline("SELECT name FROM people WHERE age > 21")

  sub("1. FILTER EARLY (predicate pushdown): the filter is written AFTER the join, executed BEFORE it")
  val cities = Seq(("Brussels", "BE-BRU"), ("Ghent", "BE-VOV")).toDF("city", "region")
  def joined = peopleDf.join(cities, peopleDf("city") === cities("city")).where($"age" > 60).select($"name", $"region")
  spark.optimizerEnabled = false; spark.measure("optimizer OFF") { joined.collect() }
  spark.optimizerEnabled = true;  spark.measure("optimizer ON ") { joined.collect() }
  println("   ^ 'rows entering joins': the expensive join only sees the people that pass the filter.")

  sub("2. COLUMN PRUNING: read only the columns the query needs")
  def names = peopleDf.where($"age" > 21).select($"name")
  spark.optimizerEnabled = false; spark.measure("optimizer OFF") { names.collect() }
  spark.optimizerEnabled = true;  spark.measure("optimizer ON ") { names.collect() }
  names.explain(true)
  println("   ^ 'values read': 2 of the 4 columns. An RDD[Person] always loads complete objects.")

  sub("3. PARTITION PRUNING: data stored in one directory per date; a filter on date skips whole directories")
  spark.fs.write("events/date=2025-11-04/part-0.csv", Seq("alice,login", "bob,login", "alice,logout"))
  spark.fs.write("events/date=2025-11-05/part-0.csv", Seq("carol,login", "alice,login"))
  spark.fs.write("events/date=2025-11-06/part-0.csv", Seq("bob,login", "dave,login", "erin,login", "bob,logout"))
  val events = spark.read.schema("user STRING, action STRING").csv("events")
  events.printSchema()
  def onTheFifth = events.where($"date" === "2025-11-05" && $"action" === "login")
  spark.optimizerEnabled = false; spark.measure("optimizer OFF") { onTheFifth.collect() }
  spark.optimizerEnabled = true;  spark.measure("optimizer ON ") { onTheFifth.collect() }
  onTheFifth.explain(true)

  sub("4. PHYSICAL PLANNING with a cost model: several ways to execute a join, statistics decide")
  val orders = (1 to 5000).map(i => (i, i % 1000, i * 1.5)).toDF("order_id", "customer_id", "price")
  val customers = (0 until 1000).map(i => (i, s"customer$i")).toDF("customer_id", "name")
  val bigJoinBig = orders.join(customers, orders("customer_id") === customers("customer_id"))
  println("   5000 orders x 1000 customers, both sides 'big':")
  bigJoinBig.explain()
  println("   the same join when only 10 customers survive a filter: the small side can be broadcast, no shuffle of the orders")
  orders.join(customers.where($"customer_id" < 10), orders("customer_id") === customers("customer_id")).explain()
}

// -----------------------------------------------------------------------------------------------------
// 17. TUNGSTEN
//     Applied automatically with DataFrames / SQL: off-heap memory management (no JVM objects, no garbage
//     collection), cache-aware binary processing, whole-stage code generation (operators fused into one
//     function, JIT-compiled to bytecode). Details are beyond the scope of the course.
//     Mixing RDD code and DataFrames is possible but costs: (de)serialization at every boundary between
//     binary Tungsten rows and JVM objects, the code-generation pipeline is cut in pieces, and Catalyst
//     cannot optimize the opaque part.
// -----------------------------------------------------------------------------------------------------
def s17_tungsten(): Unit = section(17, "Tungsten, and the price of mixing in opaque code") {
  val spark = new SparkSession(); import spark.implicits.*
  val peopleDf = (1 to 1000).map(i => (s"person$i", 15 + i % 50, i * 10.0)).toDF("name", "age", "salary")

  sub("all declarative: one optimizable plan")
  val declarative = peopleDf.select($"name", $"age").where($"age" > 60)
  spark.measure("declarative") { declarative.collect() }
  declarative.explain()

  sub("the same logic with a lambda in the middle (map on a DataFrame): a wall for the optimizer")
  val mixed = peopleDf.map(row => (row.getString(0), row.getInt(1))).toDF("name", "age").where($"age" > 60)
  spark.measure("with a lambda") { mixed.collect() }
  mixed.explain()
  println("   ^ the filter is stuck ABOVE the black box and ALL columns are read below it: Spark cannot know which")
  println("     columns the lambda touches. In real Spark each such boundary also deserializes binary rows into JVM")
  println("     objects and serializes them back, and breaks whole-stage code generation.")
}

// -----------------------------------------------------------------------------------------------------
// 18. SQL
//     def sql(sqlText: String): DataFrame                    run a SQL string, get a DataFrame
//     def createOrReplaceTempView(viewName: String): Unit    make a DataFrame visible as a table name
// -----------------------------------------------------------------------------------------------------
def s18_sql(): Unit = section(18, "SQL: spark.sql and temp views") {
  val spark = new SparkSession(); import spark.implicits.*

  sub("the first example of section 9, as SQL")
  val salesDataDf = salesData.toDF("category", "store", "amount")
  salesDataDf.createOrReplaceTempView("sales")
  val salesDf = spark.sql("""
    SELECT category, amount
    FROM sales
    WHERE amount > 100
    ORDER BY amount DESC
  """)
  salesDf.show()

  sub("SQL and the DataFrame API are two front-ends for the SAME plans")
  val viaApi = salesDataDf.select($"category", $"amount").where($"amount" > 100).orderBy($"amount".desc)
  println("   optimized plan of the SQL string   : " + salesDf.optimizedPlanString.replace("\n", "  "))
  println("   optimized plan of the DataFrame API: " + viaApi.optimizedPlanString.replace("\n", "  "))

  sub("the motivational example as SQL")
  val demographicsDf = demographicsData(3000).toDF()
  val financesDf = financesData(3000).toDF()
  demographicsDf.createOrReplaceTempView("demographics")
  financesDf.createOrReplaceTempView("finances")
  val resultDf = spark.sql("""
    SELECT COUNT(*)
    FROM demographics d
    JOIN finances f ON d.id = f.id
    WHERE
        f.hasDebt AND
        f.hasFinancialDependents AND
        d.country = 'Belgium'
    """)
  // SELECT COUNT(*) gives a DataFrame with ONE row and ONE column: take the row, read its first column as a Long
  val result = resultDf.first().getLong(0);
  println(s"   result = $result")

  sub("a tour of the supported syntax")
  println("   DISTINCT:");                       spark.sql("SELECT DISTINCT store FROM sales").show()
  println("   GROUP BY + aggregates + HAVING + ORDER BY:")
  spark.sql("SELECT store, COUNT(*) AS n, SUM(amount) AS total, ROUND(AVG(amount)) AS average FROM sales GROUP BY store HAVING total > 100 ORDER BY total DESC").show()
  println("   subquery + LIMIT:")
  spark.sql("SELECT category, double_amount FROM (SELECT category, amount * 2 AS double_amount FROM sales) doubled ORDER BY double_amount DESC LIMIT 2").show()
  println("   (real Spark SQL also has window functions (OVER), INSERT / UPDATE / DELETE / MERGE, CREATE / ALTER / DROP TABLE,")
  println("    CREATE VIEW and a large function library. This mini parser only knows SELECT queries.)")

  sub("the default dialect is strict ANSI SQL: errors instead of silent NULLs")
  atRuntime("SELECT amount / 0 FROM sales") { spark.sql("SELECT amount / 0 FROM sales").show() }
  atRuntime("SELECT CAST(category AS INT) FROM sales") { spark.sql("SELECT CAST(category AS INT) FROM sales").show() }
  atRuntime("SELECT amount AS order FROM sales   (ORDER is a reserved keyword)") { spark.sql("SELECT amount AS order FROM sales").show() }
  println("   the permissive dialect (spark.sql.ansi.enabled = false) returns NULL instead:")
  spark.conf.set("spark.sql.ansi.enabled", "false")
  spark.sql("SELECT amount / 0 AS divided, CAST(category AS INT) AS casted FROM sales LIMIT 2").show()
  spark.conf.set("spark.sql.ansi.enabled", "true")

  sub("SQL strings vs the DataFrame API")
  atRuntime("a typo inside a SQL string is found at run time") { spark.sql("SELECT category amount FROM sales WERE amount > 100") }
  println("   does `salesDataDf.select($\"category\" $\"amount\")` (a missing comma) compile? " + compiles("""salesDataDf.select($"category" $"amount")"""))
  println("   The API lets you build queries step by step in Scala, and the compiler at least checks the syntax.")
}

// -----------------------------------------------------------------------------------------------------
// 19. LIMITATIONS OF DATAFRAMES
//     (1) not statically typed: column names and types are only checked at run time
//     (2) they need (semi-)structured data: everything must fit a schema. Truly unstructured data -> RDDs
// -----------------------------------------------------------------------------------------------------
def s19_limitations(): Unit = section(19, "Limitations of DataFrames") {
  val spark = new SparkSession(); import spark.implicits.*
  val messagesDf = Seq(("hello #spark", "#spark", "@bob")).toDF("text", "hashTags", "user_mentions")
  println("   does `messagesDf.select($\"hasTags\")` compile? " + compiles("""messagesDf.select($"hasTags")""") + "   (typo: hasTags instead of hashTags)")
  atRuntime("""messagesDf.select($"hasTags")""") { messagesDf.select($"hasTags") }
  println("   With RDDs of case classes you get this kind of error at compile time:")
  case class Message(text: String, hashTags: String)
  val messagesRdd: RDD[Message] = spark.sparkContext.parallelize(Seq(Message("hello #spark", "#spark")))
  println("   `messagesRdd.map(m => m.hasTags)` -> compiler: " + compileError("messagesRdd.map(m => m.hasTags)"))
}

// -----------------------------------------------------------------------------------------------------
// 20. DATASETS: motivation
//     Going from the DataFrame world to the Scala world means casting: a Row only hands out `Any`.
//     In Scala a DataFrame is not its own class:   type DataFrame = Dataset[Row]
//     So there is ONE abstraction, the Dataset. A DataFrame is a Dataset of generic, dynamically typed Rows;
//     a Dataset[Sale] is a Dataset of statically typed case class objects.
// -----------------------------------------------------------------------------------------------------
def s20_datasetsMotivation(): Unit = section(20, "Datasets: motivation. Row, casts, and DataFrame = Dataset[Row]") {
  val spark = new SparkSession(); import spark.implicits.*

  sub("the motivational example: average sales amount per store - compiles fine, contains a mistake")
  val salesDf = salesData.toDF("category", "store", "amount")
  val averageSalesDf = salesDf.groupBy($"store").avg("amount")
  averageSalesDf.show()
  val mapped = averageSalesDf.map { row =>
    try {
      // wrong column index, so casting String to Double
      val avgAmount: Double = row.getDouble(0)
      s"Average amount: EUR ${avgAmount.round}"
    } catch {
      case e: Exception => s"Error: ${e.getMessage}"
    }
  }
  mapped.collect().foreach(println)
  println("   with the right index it works, but the cast is still there: you repeat type information that the")
  println("   schema already has, and it silently breaks when the schema changes.")

  sub("interacting with a Row")
  val row = Row(1, true, "a string", null)
  val firstValue = row(0)                     // firstValue: Any = 1
  val fourthValue = row(3)                    // fourthValue: Any = null
  val firstValueTyped = row.getInt(0)         // firstValueTyped: Int = 1
  val isNull = row.isNullAt(3)                // isNull: Boolean = true
  println(s"   row(0) = $firstValue (static type Any)   row(3) = $fourthValue   row.getInt(0) = $firstValueTyped   row.isNullAt(3) = $isNull")
  println("   does `val n: Int = row(0)` compile?  " + compiles("val n: Int = row(0)") + "   <- the compiler only knows: Any")
  atRuntime("row.getInt(2)   <- compiles, but column 2 holds a String") { row.getInt(2) }

  sub("type DataFrame = Dataset[Row]")
  val proof = summon[DataFrame =:= Dataset[Row]]                 // only compiles because the two types are identical
  val sameThing: Dataset[Row] = salesDf
  println("   a DataFrame IS a Dataset[Row]: " + proof)
}

// -----------------------------------------------------------------------------------------------------
// 21. CREATING A DATASET: from a collection, from an RDD (both toDS), from a DataFrame (as[T], a dynamic cast)
// -----------------------------------------------------------------------------------------------------
def s21_creatingDatasets(): Unit = section(21, "Creating Datasets: toDS and as[T]") {
  val spark = new SparkSession(); import spark.implicits.*
  val sc = spark.sparkContext

  // from collection
  val salesCollection: Seq[Sale] = salesData.map {
    case (category, store, amount) => Sale(category, store, amount)
  }
  val dsFromCollection: Dataset[Sale] = salesCollection.toDS()

  // from RDD
  val salesRdd: RDD[Sale] = sc.parallelize(salesCollection)
  val dsFromRdd: Dataset[Sale] = salesRdd.toDS()

  // from DataFrame
  val salesDf = salesData.toDF("category", "store", "amount")
  val dsFromDf: Dataset[Sale] = salesDf.as[Sale]

  dsFromDf.printSchema()
  println("   all three hold the same data: " + (dsFromCollection.collect().toList == dsFromRdd.collect().toList && dsFromRdd.collect().toList == dsFromDf.collect().toList))
  val firstSale: Sale = dsFromDf.first()                          // a Sale, not a Row!
  println(s"   first() returns a ${firstSale.getClass.getSimpleName}: $firstSale -> firstSale.amount is a Double: ${firstSale.amount}")

  sub("as[T] is a DYNAMIC cast: the compiler believes you, the check against the schema happens at run time")
  val wrongDf = salesData.toDF("category", "shop", "amount")     // column is called shop, Sale wants store
  println("   does `wrongDf.as[Sale]` compile? " + compiles("wrongDf.as[Sale]"))
  atRuntime("wrongDf.as[Sale]") { wrongDf.as[Sale] }
}

// -----------------------------------------------------------------------------------------------------
// 22. TRANSFORMATIONS ON DATASETS
//     A Dataset has BOTH APIs:
//       dynamically typed (Column based) : select, where, groupBy, join ... everything a DataFrame has
//       statically typed (lambda based)  : map, flatMap, filter(lambda), groupByKey ... like an RDD
//     Typed code is checked by the compiler, but a lambda is a black box for Catalyst again.
// -----------------------------------------------------------------------------------------------------
def s22_datasetTransformations(): Unit = section(22, "Dataset transformations: typed and untyped, and what the optimizer sees") {
  val spark = new SparkSession(); import spark.implicits.*
  val salesDs: Dataset[Sale] = salesData.toDF("category", "store", "amount").as[Sale]
  val salesDf: DataFrame = salesDs.toDF()

  sub("statically typed transformations: RDD-like higher-order functions, checked by the compiler")
  val withVat: Dataset[Double] = salesDs.map(sale => sale.amount * 1.21)
  val expensive: Dataset[Sale] = salesDs.filter(sale => sale.amount > 100)
  val words: Dataset[String]   = salesDs.flatMap(sale => List(sale.category, sale.store))
  println("   map     : " + withVat.collect().mkString(", "))
  println("   filter  : " + expensive.collect().mkString(", "))
  println("   flatMap : " + words.collect().mkString(", "))

  sub("the typo test: the SAME mistake, DataFrame vs Dataset")
  println("   DataFrame: `salesDf.map(row => row.getAs[Double](\"amont\") * 1.2)` compiles? " + compiles("""salesDf.map(row => row.getAs[Double]("amont") * 1.2)"""))
  atRuntime("   ... and then at run time") { salesDf.map(row => row.getAs[Double]("amont") * 1.2).collect() }
  println("   Dataset  : `salesDs.map(sale => sale.amont * 1.2)` compiles? " + compiles("salesDs.map(sale => sale.amont * 1.2)"))
  println("      -> compiler: " + compileError("salesDs.map(sale => sale.amont * 1.2)"))

  sub("dynamically typed transformations also work on a Dataset, but the static type is gone afterwards")
  val selected: DataFrame = salesDs.select($"category", $"amount")           // Dataset[Sale] -> DataFrame (= Dataset[Row])
  println("   is salesDs.select(...) still a Dataset[Sale]? " + compiles("""val d: Dataset[Sale] = salesDs.select($"category", $"amount")"""))
  println("   both APIs mix freely: " + salesDs.where($"amount" > 40).map(_.store).distinct().collect().mkString(", "))

  sub("the price of lambdas: Column filter vs lambda filter under a join")
  val stores = Seq(("Brussels", "BE"), ("Ghent", "BE"), ("Antwerp", "BE")).toDF("store", "country")
  println("   salesDs.join(stores, ...).where($\"amount\" > 100)   - Catalyst pushes the filter below the join and prunes columns:")
  salesDs.join(stores, salesDs("store") === stores("store")).where($"amount" > 100).select($"category", $"country").explain(true)
  println("   salesDs.filter(sale => sale.amount > 100) written as a typed lambda AFTER a typed map - nothing can move:")
  salesDs.map(sale => sale.copy(amount = sale.amount)).filter(sale => sale.amount > 100).toDF().select($"category").explain(true)
}

// -----------------------------------------------------------------------------------------------------
// 23. GROUPING AND AGGREGATION ON DATASETS
//     def groupByKey[K](func: T => K): KeyValueGroupedDataset[K, T]     the key comes from a (typed) FUNCTION
//     KeyValueGroupedDataset is again a 'waiting' object. On it:
//       def agg[U](col: TypedColumn[V, U]): Dataset[(K, U)]             declarative aggregation, optimizable
//       def mapValues[W](f: V => W): KeyValueGroupedDataset[K, W]
//       def mapGroups[U](f: (K, Iterator[V]) => U): Dataset[U]          arbitrary code per group: flexible, needs a full shuffle
//       def flatMapGroups[U](f: (K, Iterator[V]) => IterableOnce[U]): Dataset[U]
//       def reduceGroups(f: (V, V) => V): Dataset[(K, V)]
// -----------------------------------------------------------------------------------------------------
def s23_datasetGrouping(): Unit = section(23, "Dataset grouping: groupByKey, agg, mapGroups, flatMapGroups, reduceGroups") {
  val spark = new SparkSession(); import spark.implicits.*
  val salesDs: Dataset[Sale] = salesData.map { case (c, s, a) => Sale(c, s, a) }.toDS();

  sub("agg with a TypedColumn: key type (String) and result type (Double) are both static")
  val totalSalesDs: Dataset[(String, Double)] = salesDs
        .groupByKey(_.store)
        .agg(sum($"amount").as[Double])       // .as[Double] turns the Column into a TypedColumn: a (dynamic) cast again
  totalSalesDs.show()
  println("   count(): " + salesDs.groupByKey(_.store).count().collect().mkString(", "))

  sub("processing whole groups with your own code (not available on a DataFrame's groupBy)")
  val perStore = salesDs.groupByKey(_.store)
  val descriptions: Dataset[String] = perStore.mapGroups { (store, sales) => val all = sales.toList; s"$store sold ${all.size} item(s), most expensive: ${all.maxBy(_.amount).category}" }
  descriptions.collect().foreach(line => println("   mapGroups     : " + line))
  val cheapAndExpensive: Dataset[(String, Double)] = perStore.flatMapGroups { (store, sales) => val amounts = sales.map(_.amount).toList; List((store, amounts.min), (store, amounts.max)).distinct }
  println("   flatMapGroups : " + cheapAndExpensive.collect().mkString(", "))
  val amountsOnly = perStore.mapValues(_.amount)                 // KeyValueGroupedDataset[String, Double]
  println("   mapValues + reduceGroups: " + amountsOnly.reduceGroups(_ + _).collect().mkString(", "))

  sub("there is no reduceByKey on a Dataset: groupByKey + reduceGroups (type-safe, but verbose and opaque)")
  val totalPerCategory: Dataset[(String, Sale)] = salesDs
                    .groupByKey(sale => sale.category)  // group (type-safe)
                    .reduceGroups { (sale1, sale2) =>   // reduce (type-safe)
                      Sale(sale1.category, "*", sale1.amount + sale2.amount)
                    }
  totalPerCategory.collect().foreach(pair => println("   " + pair))
  totalPerCategory.explain()

  sub("the common, practical equivalent of reduceByKey: declarative groupBy + agg (not statically type-safe, fully optimizable)")
  val totalSalesDF = salesDs
    .groupBy("category") // not statically type-safe
    .agg(sum("amount"))
  totalSalesDF.show()
  totalSalesDF.explain()
  println("   Typed aggregation functions were deprecated in favour of this ONE optimized aggregation path.")
  println("   Static type safety still pays off everywhere else (map, filter, ...), where the complex logic usually lives.")
}

// -----------------------------------------------------------------------------------------------------
// 24. BACK TO THE MOTIVATIONAL EXAMPLE, with Datasets
// -----------------------------------------------------------------------------------------------------
def s24_motivationalExampleWithDatasets(): Unit = section(24, "Motivational example with Datasets: average sales per store") {
  val spark = new SparkSession(); import spark.implicits.*

  val salesDs: Dataset[Sale] = salesData.toDF("category", "store", "amount").as[Sale]

  val averageSalesDs: Dataset[(String, Double)] =
    salesDs.groupByKey(_.store)
            .agg(avg("amount").as[Double])

  val mapped = averageSalesDs.map {
      case (store: String, avgAmount: Double) => s"Average amount: EUR ${avgAmount.round}"
  }

  mapped.collect().foreach(println)
  println("   no Row, no index, no getDouble: the element type (String, Double) is known to the compiler.")
  println("   the section-20 mistake cannot be written: `averageSalesDs.map { case (store, avg) => store.round }` compiles? " +
    compiles("averageSalesDs.map { case (store, avg) => store.round }"))
}

// -----------------------------------------------------------------------------------------------------
// 25. RDDs vs DATAFRAMES vs DATASETS
// -----------------------------------------------------------------------------------------------------
def s25_rddVsDataFrameVsDataset(): Unit = section(25, "RDDs vs DataFrames vs Datasets: when to use which") {
  println("""                       RDD                      DataFrame (= Dataset[Row])     Dataset[T]
   data                 anything, also unstructured   (semi-)structured, schema    (semi-)structured, schema
   element type         T, static                     Row, dynamic                 T (case class), static
   API                  functional (lambdas)          declarative (Columns, SQL)   both
   errors found at      compile time                  run time (analysis)          compile time for typed code
   optimized by         you                           Catalyst + Tungsten          Catalyst + Tungsten, except inside lambdas

   Use Datasets when   : structured/semi-structured data, you want compile-time type safety and functional APIs,
                         decent (but maybe not the best) performance is enough.
   Use DataFrames when : structured/semi-structured data and you want the best performance, optimized for you.
   Use RDDs when       : the data is unstructured, you must control low-level details of the computation,
                         or your data types cannot be serialized with Encoders.""")

  sub("the same question in the three APIs: total amount per store")
  val spark = new SparkSession(); import spark.implicits.*
  val rdd: RDD[Sale] = spark.sparkContext.parallelize(salesData.map { case (c, s, a) => Sale(c, s, a) })
  println("   RDD      : " + rdd.map(sale => (sale.store, sale.amount)).reduceByKey(_ + _).collect().mkString(", "))
  println("   DataFrame: " + rdd.toDF().groupBy("store").sum("amount").collect().mkString(", "))
  println("   Dataset  : " + rdd.toDS().groupByKey(_.store).agg(sum("amount").as[Double]).collect().mkString(", "))
}

// -----------------------------------------------------------------------------------------------------
// 26. EXERCISES of the chapter, on small made-up data
//     record format of the wiki logs:  language pageName accessCount contentSize
// -----------------------------------------------------------------------------------------------------
def s26_exercises(): Unit = section(26, "Exercises") {
  val spark = new SparkSession(); import spark.implicits.*
  spark.fs.write("wiki-logs-full.txt", Seq(
    "en Main_Page 500 20000", "en Apache_Spark 120 9000", "en.mw en 900 141180", "nl Hoofdpagina 90 15000", "it Pagina_principale 70 16000",
    "it Apache_Spark 15 3000", "es Wikipedia:Portada 260 18000", "fr Accueil 200 17000", "en Scala 80 7000", "it Roma 40 9000"))
  spark.fs.write("wiki-logs-full-2.txt", Seq(
    "en Main_Page 450 20000", "it Pagina_principale 90 16000", "it Roma 65 9000", "it Milano 30 8000", "nl Hoofdpagina 110 15000", "en Scala 100 7000"))

  sub("Exercise 1: loading and querying")
  val wiki = spark.read.option("delimiter", " ").schema("language STRING, name STRING, accesses LONG, size LONG").csv("wiki-logs-full.txt")
  wiki.printSchema()
  wiki.createOrReplaceTempView("wiki")
  println(s"   ${wiki.where($"language" === "en").count()} pages in English (DataFrame API)")
  println(s"   ${spark.sql("SELECT COUNT(*) FROM wiki WHERE language = 'en'").first().getLong(0)} pages in English (query string)")
  println(s"   ${spark.sparkContext.textFile("wiki-logs-full.txt").map(_.split(" ")).filter(fields => fields(0) == "en").count()} pages in English (RDD)")

  sub("Exercise 2: grouping (each query with the DataFrame API and with SQL)")
  println("   max size     " + wiki.select(max("size")).first().getLong(0))
  println("   max size sql " + spark.sql("SELECT MAX(size) FROM wiki").first().getLong(0))
  println("   max size page     " + wiki.orderBy($"size".desc).first())
  println("   max size page sql " + spark.sql("SELECT * FROM wiki ORDER BY size DESC LIMIT 1").first())
  println("   max access page name     " + wiki.orderBy($"accesses".desc).select("name").first().getString(0))
  println("   max access page name sql " + spark.sql("SELECT name FROM wiki ORDER BY accesses DESC LIMIT 1").first().getString(0))
  println("   max lang total accesses     " + wiki.groupBy("language").agg(sum("accesses").as("total")).orderBy($"total".desc).first().getString(0))
  println("   max lang total accesses sql " + spark.sql("SELECT language, SUM(accesses) AS total FROM wiki GROUP BY language ORDER BY total DESC LIMIT 1").first().getString(0))

  sub("Exercise 3: joining two periods")
  val wiki2 = spark.read.option("delimiter", " ").schema("language STRING, name STRING, accesses LONG, size LONG").csv("wiki-logs-full-2.txt")
  val bothPeriods = wiki.join(wiki2, wiki("language") === wiki2("language") && wiki("name") === wiki2("name"))
    .select(wiki("language"), wiki("name"), (wiki("accesses") + wiki2("accesses")).as("total_accesses"))
  println("   Italian pages, accesses summed over the two periods:")
  bothPeriods.where($"language" === "it").show()
  println("   most accessed page over both periods: " + bothPeriods.orderBy($"total_accesses".desc).first())
  println("   (an inner join only keeps pages that occur in BOTH files; it.Milano and it.Apache_Spark are dropped)")

  sub("Exercise 4: grouping and aggregation on events.csv (organizer name budget)")
  val organizers = List("Org1" -> 2337.0, "Org2" -> 866.0, "Org3" -> 2664.0)
  spark.fs.write("events.csv", for (i <- 1 to 9; (org, budget) = organizers(i % 3) if !(org == "Org3" && i > 6)) yield s"$org,event$i,${budget + (i % 2) * 100 - 50}")
  val eventsDf = spark.read.schema("organizer STRING, name STRING, budget DOUBLE").csv("events.csv")
  eventsDf.groupBy("organizer")
    .agg(count("*").as("number_of_events"), round(avg("budget")).as("avg_budget_per_event"))
    .orderBy($"number_of_events".desc, $"avg_budget_per_event".desc)
    .show()
}

// =====================================================================================================
//  PART 2 - MINI SPARK SQL, the engine behind the demos.
//  Reading order: types + Row -> expressions + Column -> logical plans (L) -> Analyzer -> Optimizer
//                 -> Engine (execution) -> Planner (physical plan) -> Dataset -> SqlParser -> SparkSession
// =====================================================================================================

// ----- Spark SQL types and schemas (section 7) -----------------------------------------------------------
sealed trait DataType { def simple: String }
case object StringType  extends DataType { val simple = "string" }
case object IntegerType extends DataType { val simple = "integer" }
case object LongType    extends DataType { val simple = "long" }
case object DoubleType  extends DataType { val simple = "double" }
case object BooleanType extends DataType { val simple = "boolean" }
case object NullType    extends DataType { val simple = "void" }
final case class ArrayType(elementType: DataType, containsNull: Boolean = true) extends DataType { val simple = "array" }
final case class MapType(keyType: DataType, valueType: DataType, valueContainsNull: Boolean = true) extends DataType { val simple = "map" }
final case class StructField(name: String, dataType: DataType, nullable: Boolean = true)        // ONE column
final case class StructType(fields: Seq[StructField]) extends DataType {                         // the whole schema
  val simple = "struct"
  def fieldNames: Seq[String] = fields.map(_.name)
  def fieldIndex(name: String): Int = fields.indexWhere(_.name == name) match {
    case -1 => throw new IllegalArgumentException(s"""Field "$name" does not exist. Available fields: ${fieldNames.mkString(", ")}""")
    case i  => i
  }
  def treeString: String = "root\n" + StructType.tree(this, " |")
  def printTreeString(): Unit = print(treeString)
}
object StructType {
  def tree(dataType: DataType, prefix: String): String = dataType match {
    case StructType(fields)       => fields.map(f => s"$prefix-- ${f.name}: ${f.dataType.simple} (nullable = ${f.nullable})\n" + tree(f.dataType, prefix + "    |")).mkString
    case ArrayType(element, nul)  => s"$prefix-- element: ${element.simple} (containsNull = $nul)\n" + tree(element, prefix + "    |")
    case MapType(key, value, nul) => s"$prefix-- key: ${key.simple}\n$prefix-- value: ${value.simple} (valueContainsNull = $nul)\n" + tree(value, prefix + "    |")
    case _                        => ""
  }
  def of(output: Seq[Attribute]): StructType = StructType(output.map(a => StructField(a.name, a.dataType, a.nullable)))
}

// a generic record: positions and `Any`. The compiler knows nothing about what is inside (section 20).
final class Row(val values: Vector[Any], val schema: Option[StructType] = None) {
  def length: Int = values.length
  def apply(i: Int): Any = values(i)
  def get(i: Int): Any = values(i)
  def isNullAt(i: Int): Boolean = values(i) == null
  def getInt(i: Int): Int = values(i).asInstanceOf[Int]              // a CAST: the compiler trusts you, the JVM checks at run time
  def getLong(i: Int): Long = values(i).asInstanceOf[Long]
  def getDouble(i: Int): Double = values(i).asInstanceOf[Double]
  def getBoolean(i: Int): Boolean = values(i).asInstanceOf[Boolean]
  def getString(i: Int): String = values(i).asInstanceOf[String]
  def getAs[T](fieldName: String): T = schema match {
    case Some(s) => values(s.fieldIndex(fieldName)).asInstanceOf[T]
    case None    => throw new UnsupportedOperationException("fieldIndex on a Row without schema is undefined.")
  }
  override def toString: String = values.map(v => if (v == null) "null" else v.toString).mkString("[", ",", "]")
  override def equals(other: Any): Boolean = other match { case r: Row => r.values == values; case _ => false }
  override def hashCode: Int = values.hashCode
}
object Row { def apply(values: Any*): Row = new Row(values.toVector) }

// ----- encoders: how a Scala type T maps to columns (sections 6, 7, 21) ---------------------------------
// which Spark SQL type does a Scala type get when it is ONE column?
trait SqlType[T] { def dataType: DataType; def nullable: Boolean }
object SqlType {
  def of[T](dt: DataType, isNullable: Boolean = true): SqlType[T] = new SqlType[T] { def dataType = dt; def nullable = isNullable }
  given SqlType[String]  = of(StringType)
  given SqlType[Int]     = of(IntegerType, false)
  given SqlType[Long]    = of(LongType, false)
  given SqlType[Double]  = of(DoubleType, false)
  given SqlType[Boolean] = of(BooleanType, false)
  given [T](using element: SqlType[T]): SqlType[Array[T]] = of(ArrayType(element.dataType, element.nullable))
  given [T](using element: SqlType[T]): SqlType[Seq[T]]   = of(ArrayType(element.dataType, element.nullable))
  given [K, V](using key: SqlType[K], value: SqlType[V]): SqlType[Map[K, V]] = of(MapType(key.dataType, value.dataType, value.nullable))
  inline given [T <: Product](using m: Mirror.ProductOf[T]): SqlType[T] = of(struct[T])          // a nested case class = a struct column
  // "reflection": ask the compiler for the field names and field types of a case class (or tuple: _1, _2, ...)
  inline def struct[T](using m: Mirror.ProductOf[T]): StructType = {
    val names = constValueTuple[m.MirroredElemLabels].toList.map(_.toString)
    val types = summonAll[Tuple.Map[m.MirroredElemTypes, SqlType]].toList.map(_.asInstanceOf[SqlType[?]])
    StructType(names.zip(types).map { case (name, t) => StructField(name, t.dataType, t.nullable) })
  }
}
// converts between objects of type T and rows. Every Dataset[T] needs one; they are found implicitly.
trait Encoder[T] {
  def schema: StructType
  def isPrimitive: Boolean                           // one column called "value" (Int, String, ...) vs one column per field
  def toRow(value: T): Vector[Any]
  def fromRow(row: Vector[Any]): T
  def cellType: DataType = if (isPrimitive) schema.fields.head.dataType else schema
}
final class PrimitiveEncoder[T](dataType: DataType, nullable: Boolean) extends Encoder[T] {
  val schema: StructType = StructType(Seq(StructField("value", dataType, nullable))); val isPrimitive = true
  def toRow(value: T): Vector[Any] = Vector(value); def fromRow(row: Vector[Any]): T = row(0).asInstanceOf[T]
}
final class ProductEncoder[T](val schema: StructType, build: Product => T) extends Encoder[T] {
  val isPrimitive = false
  def toRow(value: T): Vector[Any] = value.asInstanceOf[Product].productIterator.toVector
  def fromRow(row: Vector[Any]): T = build(Tuple.fromArray(row.toArray[Any]))
}
final class RowEncoder(val schema: StructType) extends Encoder[Row] {
  val isPrimitive = false
  def toRow(value: Row): Vector[Any] = value.values; def fromRow(row: Vector[Any]): Row = new Row(row, Some(schema))
}
object Encoder {
  given Encoder[String]  = new PrimitiveEncoder(StringType, true)
  given Encoder[Int]     = new PrimitiveEncoder(IntegerType, false)
  given Encoder[Long]    = new PrimitiveEncoder(LongType, false)
  given Encoder[Double]  = new PrimitiveEncoder(DoubleType, false)
  given Encoder[Boolean] = new PrimitiveEncoder(BooleanType, false)
  inline given product[T <: Product](using m: Mirror.ProductOf[T]): Encoder[T] = new ProductEncoder[T](SqlType.struct[T], p => m.fromProduct(p))
  def tuple[A, B](a: Encoder[A], b: Encoder[B], nameA: String, nameB: String): Encoder[(A, B)] = new Encoder[(A, B)] {
    val schema: StructType = StructType(Seq(StructField(nameA, a.cellType), StructField(nameB, b.cellType))); val isPrimitive = false
    def toRow(value: (A, B)): Vector[Any] = Vector(value._1, value._2)
    def fromRow(row: Vector[Any]): (A, B) = (row(0).asInstanceOf[A], row(1).asInstanceOf[B])
  }
}

// ----- expressions: what a Column is made of (sections 3, 10) -------------------------------------------
object Ids { private var last = -1; def next(): Int = { last += 1; last }; def reset(): Unit = last = -1 }

// a column of a concrete plan: name + type + a unique id (so two columns named "id" can be told apart)
final case class Attribute(name: String, dataType: DataType, nullable: Boolean, id: Int, qualifier: Option[String] = None)

sealed trait Expr
final case class UnresolvedAttr(nameParts: List[String]) extends Expr          // $"amount": only a NAME, not yet tied to any data
final case class Ref(attribute: Attribute) extends Expr                         // amount#2: a column of a specific plan
final case class Lit(value: Any) extends Expr
final case class Alias(child: Expr, name: String, id: Int = Ids.next()) extends Expr
final case class BinOp(op: String, left: Expr, right: Expr) extends Expr        // + - * /  = != < <= > >=  AND OR
final case class Not(child: Expr) extends Expr
final case class Func(name: String, args: Seq[Expr]) extends Expr               // round, lower, upper, isnull
final case class Agg(function: String, child: Expr) extends Expr                // sum avg count min max: over a GROUP of rows
final case class Cast(child: Expr, to: DataType) extends Expr
final case class SortOrder(child: Expr, ascending: Boolean) extends Expr
case object Star extends Expr

object Expr {
  def children(e: Expr): Seq[Expr] = e match {
    case Alias(c, _, _) => Seq(c); case BinOp(_, l, r) => Seq(l, r); case Not(c) => Seq(c); case Func(_, args) => args
    case Agg(_, c) => Seq(c); case Cast(c, _) => Seq(c); case SortOrder(c, _) => Seq(c); case _ => Nil
  }
  def transform(e: Expr)(rule: PartialFunction[Expr, Expr]): Expr = {            // rewrite a tree bottom-up: THE tool of an optimizer
    def go(x: Expr): Expr = transform(x)(rule)
    val rebuilt = e match {
      case a: Alias => a.copy(child = go(a.child)); case BinOp(op, l, r) => BinOp(op, go(l), go(r)); case Not(c) => Not(go(c))
      case Func(n, args) => Func(n, args.map(go)); case Agg(f, c) => Agg(f, go(c)); case Cast(c, t) => Cast(go(c), t)
      case SortOrder(c, asc) => SortOrder(go(c), asc); case leaf => leaf
    }
    rule.applyOrElse(rebuilt, identity[Expr])
  }
  def collect[A](e: Expr)(pick: PartialFunction[Expr, A]): List[A] = pick.lift(e).toList ++ children(e).flatMap(collect(_)(pick))
  def refs(e: Expr): Set[Int] = collect(e) { case Ref(a) => a.id }.toSet        // which columns does this expression use?
  def hasAggregate(e: Expr): Boolean = collect(e) { case a: Agg => a }.nonEmpty
  def conjuncts(e: Expr): List[Expr] = e match { case BinOp("AND", l, r) => conjuncts(l) ++ conjuncts(r); case other => List(other) }
  def and(parts: Seq[Expr]): Option[Expr] = parts.reduceOption((a, b) => BinOp("AND", a, b))
  // for a join: is this conjunct "leftExpression = rightExpression"? Then a hash join can use it.
  def equiKey(e: Expr, left: Set[Int], right: Set[Int]): Option[(Expr, Expr)] = e match {
    case BinOp("=", a, b) if refs(a).nonEmpty && refs(b).nonEmpty && refs(a).subsetOf(left) && refs(b).subsetOf(right) => Some((a, b))
    case BinOp("=", a, b) if refs(a).nonEmpty && refs(b).nonEmpty && refs(a).subsetOf(right) && refs(b).subsetOf(left) => Some((b, a))
    case _ => None
  }
  // ids = true: as shown in plans (amount#2). ids = false: the column name Spark derives, e.g. round((amount * 0.21), 0)
  def str(e: Expr, ids: Boolean): String = {
    def s(x: Expr): String = str(x, ids)
    e match {
      case UnresolvedAttr(parts) => (if (ids) "'" else "") + parts.mkString(".")
      case Ref(a)                => if (ids) s"${a.name}#${a.id}" else a.name
      case Lit(null)             => "NULL"
      case Lit(v)                => v.toString
      case Alias(c, name, id)    => if (ids) s"${s(c)} AS $name#$id" else name
      case BinOp(op, l, r)       => s"(${s(l)} $op ${s(r)})"
      case Not(c)                => s"(NOT ${s(c)})"
      case Func("round", Seq(c)) => s"round(${s(c)}, 0)"
      case Func(name, args)      => s"$name(${args.map(s).mkString(", ")})"
      case Agg("count", Star)    => "count(1)"
      case Agg(function, c)      => s"$function(${s(c)})"
      case Cast(c, to)           => s"CAST(${s(c)} AS ${to.simple.toUpperCase})"
      case SortOrder(c, asc)     => s"${s(c)} ${if (asc) "ASC" else "DESC"}"
      case Star                  => "*"
    }
  }
  def typeOf(e: Expr): DataType = e match {
    case Ref(a) => a.dataType
    case Lit(v) => v match { case _: String => StringType; case _: Int => IntegerType; case _: Long => LongType; case _: Double => DoubleType; case _: Boolean => BooleanType; case _ => NullType }
    case Alias(c, _, _) => typeOf(c)
    case BinOp("+" | "-" | "*", l, r) => (typeOf(l), typeOf(r)) match { case (IntegerType, IntegerType) => IntegerType; case (IntegerType | LongType, IntegerType | LongType) => LongType; case _ => DoubleType }
    case BinOp("/", _, _) => DoubleType
    case _: BinOp | _: Not | Func("isnull", _) => BooleanType
    case Func("lower" | "upper", _) => StringType
    case Func(_, args) => args.headOption.fold(NullType: DataType)(typeOf)
    case Agg("count", _) => LongType
    case Agg("avg", _) => DoubleType
    case Agg("sum", c) => if (typeOf(c) == DoubleType) DoubleType else LongType
    case Agg(_, c) => typeOf(c)
    case Cast(_, to) => to
    case SortOrder(c, _) => typeOf(c)
    case _ => NullType
  }
  def toAttribute(e: Expr): Attribute = e match {
    case Ref(a)             => a
    case Alias(c, name, id) => Attribute(name, typeOf(c), true, id)
    case other              => throw new AnalysisException(s"expression ${str(other, true)} has no name yet")
  }
}

// A Column is a small wrapper around an expression TREE. Operators build bigger trees; nothing is computed.
class Column(val expr: Expr) {
  private def binary(op: String, other: Any): Column = new Column(BinOp(op, expr, Column.toExpr(other)))
  def +(other: Any): Column = binary("+", other);  def -(other: Any): Column = binary("-", other)
  def *(other: Any): Column = binary("*", other);  def /(other: Any): Column = binary("/", other)
  def >(other: Any): Column = binary(">", other);  def >=(other: Any): Column = binary(">=", other)
  def <(other: Any): Column = binary("<", other);  def <=(other: Any): Column = binary("<=", other)
  def ===(other: Any): Column = binary("=", other); def =!=(other: Any): Column = binary("!=", other)
  def &&(other: Any): Column = binary("AND", other); def ||(other: Any): Column = binary("OR", other)
  def unary_! : Column = new Column(Not(expr))
  def isNull: Column = new Column(Func("isnull", Seq(expr))); def isNotNull: Column = new Column(Not(Func("isnull", Seq(expr))))
  def as(alias: String): Column = new Column(Alias(expr, alias))
  def as[U](using encoder: Encoder[U]): TypedColumn[Any, U] = new TypedColumn[Any, U](expr, encoder)      // "I promise the result is a U"
  def asc: Column = new Column(SortOrder(expr, true)); def desc: Column = new Column(SortOrder(expr, false))
  def cast(to: DataType): Column = new Column(Cast(expr, to))
  override def toString: String = Expr.str(expr, ids = false)
}
object Column { def toExpr(value: Any): Expr = value match { case c: Column => c.expr; case literal => Lit(literal) } }
class TypedColumn[-T, U](expr: Expr, val encoder: Encoder[U]) extends Column(expr)

// real Spark: org.apache.spark.sql.functions
object functions {
  def col(name: String): Column = new Column(UnresolvedAttr(name.split('.').toList))
  def lit(value: Any): Column = new Column(Lit(value))
  def round(c: Column): Column = new Column(Func("round", Seq(c.expr)))
  def round(c: Column, scale: Int): Column = new Column(Func("round", Seq(c.expr, Lit(scale))))
  def lower(c: Column): Column = new Column(Func("lower", Seq(c.expr)));  def upper(c: Column): Column = new Column(Func("upper", Seq(c.expr)))
  private def aggregate(function: String, c: Column): Column = new Column(Agg(function, c.expr))
  def sum(c: Column): Column = aggregate("sum", c);  def sum(name: String): Column = aggregate("sum", col(name))
  def avg(c: Column): Column = aggregate("avg", c);  def avg(name: String): Column = aggregate("avg", col(name))
  def mean(c: Column): Column = aggregate("avg", c); def mean(name: String): Column = aggregate("avg", col(name))
  def min(c: Column): Column = aggregate("min", c);  def min(name: String): Column = aggregate("min", col(name))
  def max(c: Column): Column = aggregate("max", c);  def max(name: String): Column = aggregate("max", col(name))
  def count(c: Column): Column = aggregate("count", c)
  def count(name: String): Column = new Column(Agg("count", if (name == "*") Star else UnresolvedAttr(List(name))))
}

// ----- logical plans: WHAT to compute, as a tree of relational operators (section 16) -------------------
sealed trait LogicalPlan { def output: Seq[Attribute]; def children: Seq[LogicalPlan] }
object L {
  // a source of rows. `output` = the columns that are actually read; the optimizer may shrink it (column pruning)
  // and may restrict the partitions that are read (partition pruning).
  final case class Relation(name: String, fullOutput: Seq[Attribute], data: Vector[Vector[Any]], output: Seq[Attribute],
                            partitionedBy: Option[String] = None, partitionFilter: Option[Set[Any]] = None) extends LogicalPlan { def children = Nil }
  final case class UnresolvedRelation(name: String) extends LogicalPlan { def output = Nil; def children = Nil }      // FROM sales: just a NAME
  final case class SubqueryAlias(alias: String, child: LogicalPlan) extends LogicalPlan { def output = child.output.map(_.copy(qualifier = Some(alias))); def children = Seq(child) }
  final case class Project(exprs: Seq[Expr], child: LogicalPlan) extends LogicalPlan { def output = exprs.map(Expr.toAttribute); def children = Seq(child) }
  final case class Filter(condition: Expr, child: LogicalPlan) extends LogicalPlan { def output = child.output; def children = Seq(child) }
  final case class Sort(order: Seq[SortOrder], child: LogicalPlan) extends LogicalPlan { def output = child.output; def children = Seq(child) }
  final case class Aggregate(grouping: Seq[Expr], aggregates: Seq[Expr], child: LogicalPlan) extends LogicalPlan { def output = aggregates.map(Expr.toAttribute); def children = Seq(child) }
  final case class Join(left: LogicalPlan, right: LogicalPlan, joinType: String, condition: Option[Expr]) extends LogicalPlan { def output = left.output ++ right.output; def children = Seq(left, right) }
  final case class Limit(n: Int, child: LogicalPlan) extends LogicalPlan { def output = child.output; def children = Seq(child) }
  final case class Distinct(child: LogicalPlan) extends LogicalPlan { def output = child.output; def children = Seq(child) }
  // user code (a lambda) inside the plan: Catalyst can run it, but cannot look inside it
  final case class Opaque(name: String, output: Seq[Attribute], child: LogicalPlan, run: Vector[Vector[Any]] => Vector[Vector[Any]]) extends LogicalPlan { def children = Seq(child) }
}

object Plans {
  def mapChildren(plan: LogicalPlan, f: LogicalPlan => LogicalPlan): LogicalPlan = plan match {
    case L.SubqueryAlias(a, c) => L.SubqueryAlias(a, f(c)); case L.Project(e, c) => L.Project(e, f(c)); case L.Filter(e, c) => L.Filter(e, f(c))
    case L.Sort(o, c) => L.Sort(o, f(c)); case L.Aggregate(g, a, c) => L.Aggregate(g, a, f(c)); case L.Join(l, r, t, c) => L.Join(f(l), f(r), t, c)
    case L.Limit(n, c) => L.Limit(n, f(c)); case L.Distinct(c) => L.Distinct(f(c)); case o: L.Opaque => o.copy(child = f(o.child)); case leaf => leaf
  }
  def transformDown(plan: LogicalPlan)(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan =
    mapChildren(rule.applyOrElse(plan, identity[LogicalPlan]), transformDown(_)(rule))
  private def list(exprs: Seq[Expr]): String = exprs.map(Expr.str(_, ids = true)).mkString("[", ", ", "]")
  private def attrs(output: Seq[Attribute]): String = output.map(a => s"${a.name}#${a.id}").mkString("[", ", ", "]")
  def describe(plan: LogicalPlan): String = plan match {
    case r: L.Relation =>
      val pruned = if (r.output.size < r.fullOutput.size) s"   <- column pruning: ${r.output.size} of ${r.fullOutput.size} columns read" else ""
      val partitions = r.partitionFilter.fold("")(values => s"   <- partition pruning: only ${r.partitionedBy.get} = ${values.mkString(", ")} is read")
      s"${r.name} ${attrs(r.output)}$pruned$partitions"
    case L.UnresolvedRelation(name)  => s"'UnresolvedRelation [$name]"
    case L.SubqueryAlias(alias, _)   => s"SubqueryAlias $alias"
    case L.Project(exprs, _)         => s"Project ${list(exprs)}"
    case L.Filter(condition, _)      => s"Filter ${Expr.str(condition, ids = true)}"
    case L.Sort(order, _)            => s"Sort ${list(order)}"
    case L.Aggregate(grouping, a, _) => s"Aggregate ${list(grouping)}, ${list(a)}"
    case L.Join(_, _, joinType, c)   => s"Join ${joinType.split("_").map(_.capitalize).mkString}" + c.fold("")(e => ", " + Expr.str(e, ids = true))
    case L.Limit(n, _)               => s"GlobalLimit $n"
    case L.Distinct(_)               => "Distinct"
    case o: L.Opaque                 => s"${o.name} ${attrs(o.output)}"
  }
  final case class Node(text: String, children: List[Node] = Nil)
  def toNode(plan: LogicalPlan): Node = Node(describe(plan), plan.children.map(toNode).toList)
  def render(node: Node): List[String] = node.text :: node.children.zipWithIndex.flatMap { case (child, i) =>
    val last = i == node.children.size - 1; val lines = render(child)
    ((if (last) "+- " else ":- ") + lines.head) :: lines.tail.map((if (last) "   " else ":  ") + _)
  }
  def tree(plan: LogicalPlan): String = render(toNode(plan)).mkString("\n")
}

// ----- the ANALYZER: names -> concrete columns, using the schema / the catalog. Errors here = AnalysisException ----
class AnalysisException(message: String) extends Exception(message)
class ParseException(message: String) extends AnalysisException(message)

object Analyzer {
  def analyze(plan: LogicalPlan, catalog: String => Option[LogicalPlan]): LogicalPlan = {
    def go(p: LogicalPlan): LogicalPlan = p match {
      case L.UnresolvedRelation(name) => catalog(name).getOrElse(throw new AnalysisException(s"[TABLE_OR_VIEW_NOT_FOUND] The table or view `$name` cannot be found."))
      case L.SubqueryAlias(alias, child) => L.SubqueryAlias(alias, go(child))
      case L.Project(exprs, child) => val c = go(child); L.Project(expandStar(exprs, c).map(e => named(resolve(e, c.output))), c)
      case L.Filter(condition, child) =>
        val c = go(child); val resolved = resolve(condition, c.output)
        if (Expr.typeOf(resolved) != BooleanType) throw new AnalysisException(s"[DATATYPE_MISMATCH.FILTER_NOT_BOOLEAN] Filter expression '${Expr.str(resolved, false)}' of type ${Expr.typeOf(resolved).simple} is not a boolean.")
        L.Filter(resolved, c)
      case L.Sort(order, child) => go(child) match {
        // ORDER BY a column that the SELECT dropped: sort first, project afterwards
        case L.Project(exprs, below) if !order.forall(o => resolvable(o, exprs.map(Expr.toAttribute))) && order.forall(o => resolvable(o, below.output)) =>
          L.Project(exprs, L.Sort(order.map(o => resolve(o, below.output).asInstanceOf[SortOrder]), below))
        case c => L.Sort(order.map(o => resolve(o, c.output).asInstanceOf[SortOrder]), c)
      }
      case L.Aggregate(grouping, aggregates, child) =>
        val c = go(child); L.Aggregate(grouping.map(resolve(_, c.output)), aggregates.map(e => named(resolve(e, c.output))), c)
      case L.Join(left, right, joinType, condition) => val (l, r) = (go(left), go(right)); L.Join(l, r, joinType, condition.map(resolve(_, l.output ++ r.output)))
      case L.Limit(n, child) => L.Limit(n, go(child))
      case L.Distinct(child) => L.Distinct(go(child))
      case o: L.Opaque => o.copy(child = go(o.child))
      case relation => relation
    }
    go(plan)
  }
  private def expandStar(exprs: Seq[Expr], child: LogicalPlan): Seq[Expr] = exprs.flatMap { case Star => child.output.map(Ref(_)); case e => Seq(e) }
  private def named(e: Expr): Expr = e match { case _: Ref | _: Alias => e; case other => Alias(other, Expr.str(other, ids = false)) }
  private def resolvable(e: Expr, input: Seq[Attribute]): Boolean = try { resolve(e, input); true } catch { case _: AnalysisException => false }
  // THE moment a column name meets the schema. A name that is not there, or is there twice, fails here: at RUN time.
  def resolve(e: Expr, input: Seq[Attribute]): Expr = Expr.transform(e) {
    case UnresolvedAttr(parts) =>
      val candidates = parts match {
        case List(name)            => input.filter(_.name.equalsIgnoreCase(name))
        case List(qualifier, name) => input.filter(a => a.name.equalsIgnoreCase(name) && a.qualifier.exists(_.equalsIgnoreCase(qualifier)))
        case _                     => Nil
      }
      candidates.distinctBy(_.id).toList match {
        case List(attribute) => Ref(attribute)
        case Nil  => throw new AnalysisException(s"[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `${parts.mkString("`.`")}` " +
                       s"cannot be resolved. Did you mean one of the following? [${input.map(a => s"`${a.name}`").mkString(", ")}].")
        case many => throw new AnalysisException(s"[AMBIGUOUS_REFERENCE] Reference `${parts.mkString(".")}` is ambiguous, could be: [${many.map(a => s"`${a.name}`#${a.id}").mkString(", ")}].")
      }
    case Ref(a) if !input.exists(_.id == a.id) =>
      throw new AnalysisException(s"[MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT] Resolved attribute ${a.name}#${a.id} missing from ${input.map(x => s"${x.name}#${x.id}").mkString(", ")}.")
  }
}

// ----- the OPTIMIZER: rewrite the logical plan into an equivalent, cheaper one (section 16) ------------
object Optimizer {
  def optimize(plan: LogicalPlan): LogicalPlan = {
    var current = Plans.transformDown(plan) { case L.SubqueryAlias(_, child) => child }      // aliases are only needed for name resolution
    var before = ""
    while (before != Plans.tree(current)) {                                                   // apply the rules until nothing changes any more
      before = Plans.tree(current)
      current = Plans.transformDown(current)(pushDownPredicates)
    }
    val pruned = pruneColumns(current, current.output.map(_.id).toSet)
    Plans.transformDown(pruned) { case L.Project(exprs, child) if exprs == child.output.map(Ref(_)) => child }   // drop projections that do nothing
  }
  private def ids(plan: LogicalPlan): Set[Int] = plan.output.map(_.id).toSet
  private def filtered(plan: LogicalPlan, conditions: Seq[Expr]): LogicalPlan = Expr.and(conditions).fold(plan)(L.Filter(_, plan))

  // RULE 1 - filter early: move every predicate as close to the data source as possible
  private val pushDownPredicates: PartialFunction[LogicalPlan, LogicalPlan] = {
    case L.Filter(c1, L.Filter(c2, child)) => L.Filter(BinOp("AND", c2, c1), child)                                   // two filters = one filter
    case L.Filter(condition, L.Project(exprs, child)) =>                                                               // through a projection
      val definitions = exprs.collect { case Alias(definition, _, id) => id -> definition }.toMap
      L.Project(exprs, L.Filter(Expr.transform(condition) { case Ref(a) if definitions.contains(a.id) => definitions(a.id) }, child))
    case L.Filter(condition, L.Sort(order, child)) => L.Sort(order, L.Filter(condition, child))                        // sort fewer rows
    case L.Filter(condition, L.Join(left, right, joinType, joinCondition)) if joinType == "inner" || joinType == "cross" =>
      val (leftOnly, rest) = Expr.conjuncts(condition).partition(c => Expr.refs(c).subsetOf(ids(left)))               // only uses left columns -> below the join, left
      val (rightOnly, both) = rest.partition(c => Expr.refs(c).subsetOf(ids(right)))                                  // only uses right columns -> below the join, right
      val newCondition = Expr.and(joinCondition.toList ++ both)                                                        // uses both sides -> becomes (part of) the join condition
      L.Join(filtered(left, leftOnly), filtered(right, rightOnly), if (newCondition.isDefined) "inner" else joinType, newCondition)
    case L.Join(left, right, "inner", Some(condition)) if Expr.conjuncts(condition).exists(c => Expr.refs(c).subsetOf(ids(left)) || Expr.refs(c).subsetOf(ids(right))) =>
      val (leftOnly, rest) = Expr.conjuncts(condition).partition(c => Expr.refs(c).subsetOf(ids(left)))
      val (rightOnly, both) = rest.partition(c => Expr.refs(c).subsetOf(ids(right)))
      L.Join(filtered(left, leftOnly), filtered(right, rightOnly), if (both.isEmpty) "cross" else "inner", Expr.and(both))
    // RULE 3 - partition pruning: a filter "partitionColumn = literal" on partitioned data -> read only that partition
    case L.Filter(condition, relation: L.Relation) if relation.partitionFilter.isEmpty && relation.partitionedBy.exists(column => partitionValue(condition, column).isDefined) =>
      val column = relation.partitionedBy.get
      val (onPartition, others) = Expr.conjuncts(condition).partition(c => partitionValue(c, column).isDefined)
      filtered(relation.copy(partitionFilter = Some(onPartition.flatMap(c => partitionValue(c, column)).toSet)), others)
  }
  private def partitionValue(condition: Expr, column: String): Option[Any] = Expr.conjuncts(condition).collectFirst {
    case BinOp("=", Ref(a), Lit(v)) if a.name == column => v
    case BinOp("=", Lit(v), Ref(a)) if a.name == column => v
  }

  // RULE 2 - column pruning: walk down the plan with the set of columns that are still NEEDED above
  private def pruneColumns(plan: LogicalPlan, needed: Set[Int]): LogicalPlan = plan match {
    case L.Project(exprs, child) => val kept = exprs.filter(e => needed(Expr.toAttribute(e).id)); L.Project(kept, pruneColumns(child, kept.flatMap(Expr.refs).toSet))
    case L.Filter(condition, child) => L.Filter(condition, pruneColumns(child, needed ++ Expr.refs(condition)))
    case L.Sort(order, child) => L.Sort(order, pruneColumns(child, needed ++ order.flatMap(Expr.refs)))
    case L.Aggregate(grouping, aggregates, child) => L.Aggregate(grouping, aggregates, pruneColumns(child, (grouping ++ aggregates).flatMap(Expr.refs).toSet))
    case L.Join(left, right, joinType, condition) =>
      val need = needed ++ condition.toList.flatMap(Expr.refs)
      def narrow(side: LogicalPlan): LogicalPlan = {                         // do not carry unneeded columns through the (expensive) join
        val pruned = pruneColumns(side, need)
        if (pruned.output.forall(a => need(a.id))) pruned else L.Project(pruned.output.filter(a => need(a.id)).map(Ref(_)), pruned)
      }
      L.Join(narrow(left), narrow(right), joinType, condition)
    case L.Limit(n, child) => L.Limit(n, pruneColumns(child, needed))
    case L.Distinct(child) => L.Distinct(pruneColumns(child, ids(child)))
    case o: L.Opaque => o.copy(child = pruneColumns(o.child, ids(o.child)))  // a lambda might use ANY column: everything below stays
    case r: L.Relation => r.copy(output = r.output.filter(a => needed(a.id)))
    case other => other
  }
}

// ----- EXECUTION: interpret a plan over rows (real Spark: generate RDDs with Tungsten bytecode) ---------
object SQLConf { var ansiEnabled = true }

object Eval {
  private def number(v: Any): Double = v match {
    case i: Int => i.toDouble; case l: Long => l.toDouble; case d: Double => d
    case s: String => try s.trim.toDouble catch { case _: NumberFormatException =>
      if (SQLConf.ansiEnabled) throw new NumberFormatException(s"""[CAST_INVALID_INPUT] The value '$s' of the type "STRING" cannot be cast to "DOUBLE" because it is malformed.""") else Double.NaN }
    case other => throw new IllegalArgumentException(s"[DATATYPE_MISMATCH] $other is not a number")
  }
  private def integral(v: Any): Boolean = v.isInstanceOf[Int] || v.isInstanceOf[Long]
  def arithmetic(op: String, a: Any, b: Any): Any =
    if (op == "/") {
      if (number(b) == 0.0) { if (SQLConf.ansiEnabled) throw new ArithmeticException("[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead.") else null }
      else number(a) / number(b)
    } else (a, b) match {
      case (x: Int, y: Int) => op match { case "+" => x + y; case "-" => x - y; case _ => x * y }
      case _ if integral(a) && integral(b) => val (x, y) = (number(a).toLong, number(b).toLong); op match { case "+" => x + y; case "-" => x - y; case _ => x * y }
      case _ => val (x, y) = (number(a), number(b)); op match { case "+" => x + y; case "-" => x - y; case _ => x * y }
    }
  def compare(a: Any, b: Any): Int = (a, b) match {
    case (x: String, y: String)   => x.compareTo(y)
    case (x: Boolean, y: Boolean) => x.compareTo(y)
    case _                        => java.lang.Double.compare(number(a), number(b))      // a string here is cast to a number: can FAIL, at run time
  }
  def binary(op: String, a: Any, b: Any): Any = op match {                               // SQL semantics: NULL in -> NULL out
    case "AND" => if (a == false || b == false) false else if (a == true && b == true) true else null
    case "OR"  => if (a == true || b == true) true else if (a == false && b == false) false else null
    case _ if a == null || b == null => null
    case "+" | "-" | "*" | "/" => arithmetic(op, a, b)
    case "="  => compare(a, b) == 0; case "!=" => compare(a, b) != 0
    case "<"  => compare(a, b) < 0;  case "<=" => compare(a, b) <= 0
    case ">"  => compare(a, b) > 0;  case ">=" => compare(a, b) >= 0
  }
  def function(name: String, args: Seq[Any]): Any = name match {
    case "isnull" => args.head == null
    case _ if args.head == null => null
    case "round" => args.head match {
      case d: Double => BigDecimal(d).setScale(if (args.size > 1) args(1).asInstanceOf[Int] else 0, BigDecimal.RoundingMode.HALF_UP).toDouble
      case other => other }
    case "lower" => args.head.toString.toLowerCase
    case "upper" => args.head.toString.toUpperCase
    case other => throw new AnalysisException(s"[UNRESOLVED_ROUTINE] Cannot resolve function `$other`.")
  }
  def cast(v: Any, to: DataType): Any = if (v == null) null else to match {
    case StringType => v.toString
    case BooleanType => v.toString.toBoolean
    case _ => val n = try number(v) catch { case e: NumberFormatException => throw new NumberFormatException(e.getMessage.replace("DOUBLE", to.simple.toUpperCase)) }
      if (n.isNaN) null else to match { case IntegerType => n.toInt; case LongType => n.toLong; case _ => n }
  }
  // turn an expression tree into a function row => value. Column references become positions in the input row.
  def compile(e: Expr, input: Seq[Attribute]): Vector[Any] => Any = e match {
    case Ref(a) => val i = input.indexWhere(_.id == a.id); if (i < 0) throw new IllegalStateException(s"column ${a.name}#${a.id} is not in the input"); row => row(i)
    case Lit(v) => _ => v
    case Alias(c, _, _) => compile(c, input)
    case SortOrder(c, _) => compile(c, input)
    case BinOp(op, l, r) => val (left, right) = (compile(l, input), compile(r, input)); row => binary(op, left(row), right(row))
    case Not(c) => val f = compile(c, input); row => f(row) match { case b: Boolean => !b; case _ => null }
    case Func(name, args) => val fs = args.map(compile(_, input)); row => function(name, fs.map(_(row)))
    case Cast(c, to) => val f = compile(c, input); row => cast(f(row), to)
    case other => throw new IllegalStateException(s"cannot evaluate ${Expr.str(other, true)} on a single row")
  }
  // an expression with aggregate functions, evaluated over all rows of ONE group
  def aggregate(e: Expr, rows: Vector[Vector[Any]], input: Seq[Attribute]): Any = e match {
    case Agg(function, child) =>
      val values = if (child == Star) rows.map(_ => 1: Any) else { val f = compile(child, input); rows.map(f).filter(_ != null) }
      function match {
        case "count" => values.size.toLong
        case _ if values.isEmpty => null
        case "sum" => val total = values.reduce(arithmetic("+", _, _)); if (integral(total)) number(total).toLong else total
        case "avg" => values.map(number).sum / values.size
        case "min" => values.reduce((a, b) => if (compare(a, b) <= 0) a else b)
        case "max" => values.reduce((a, b) => if (compare(a, b) >= 0) a else b)
      }
    case Alias(c, _, _) => aggregate(c, rows, input)
    case BinOp(op, l, r) => binary(op, aggregate(l, rows, input), aggregate(r, rows, input))
    case Func(name, args) => function(name, args.map(aggregate(_, rows, input)))
    case Cast(c, to) => cast(aggregate(c, rows, input), to)
    case other => if (rows.isEmpty) null else compile(other, input)(rows.head)         // a grouping column: identical in every row of the group
  }
}

object Engine {
  type R = Vector[Any]
  def execute(plan: LogicalPlan, spark: SparkSession): Vector[R] = {
    def run(p: LogicalPlan): Vector[R] = p match {
      case r: L.Relation =>
        val rows = r.partitionedBy match {
          case Some(column) =>
            val at = r.fullOutput.indexWhere(_.name == column)
            val wanted = r.partitionFilter.getOrElse(r.data.map(_(at)).toSet)
            spark.inc("partitions read", wanted.size)
            r.data.filter(row => wanted(row(at)))
          case None => r.data
        }
        val positions = r.output.map(a => r.fullOutput.indexWhere(_.id == a.id))
        spark.inc("rows scanned", rows.size); spark.inc("values read", rows.size.toLong * positions.size)
        if (positions.size == r.fullOutput.size) rows else rows.map(row => positions.map(row).toVector)
      case L.SubqueryAlias(_, child) => run(child)
      case L.Project(exprs, child) => val fs = exprs.map(Eval.compile(_, child.output)); run(child).map(row => fs.map(_(row)).toVector)
      case L.Filter(condition, child) => val f = Eval.compile(condition, child.output); run(child).filter(row => f(row) == true)
      case L.Sort(order, child) =>
        val keys = order.map(o => (Eval.compile(o.child, child.output), o.ascending))
        def nullsFirst(a: Any, b: Any): Int = if (a == null && b == null) 0 else if (a == null) -1 else if (b == null) 1 else Eval.compare(a, b)
        run(child).sortWith { (x, y) => keys.iterator.map { case (key, ascending) => val c = nullsFirst(key(x), key(y)); if (ascending) c else -c }.find(_ != 0).exists(_ < 0) }
      case L.Aggregate(grouping, aggregates, child) =>
        val keyOf = grouping.map(Eval.compile(_, child.output))
        val groups = mutable.LinkedHashMap[Seq[Any], mutable.ArrayBuffer[R]]()
        run(child).foreach(row => groups.getOrElseUpdate(keyOf.map(_(row)), mutable.ArrayBuffer[R]()) += row)
        if (grouping.isEmpty && groups.isEmpty) groups(Nil) = mutable.ArrayBuffer[R]()          // SELECT COUNT(*) on no rows is still one row
        groups.values.map(rows => aggregates.map(e => Eval.aggregate(e, rows.toVector, child.output)).toVector).toVector
      case L.Join(l, r, joinType, condition) =>
        val (left, right) = (run(l), run(r))
        spark.inc("rows entering joins", left.size + right.size)
        val (leftIds, rightIds) = (l.output.map(_.id).toSet, r.output.map(_.id).toSet)
        val (equi, residual) = condition.toList.flatMap(Expr.conjuncts).partition(c => Expr.equiKey(c, leftIds, rightIds).isDefined)
        val keys = equi.flatMap(c => Expr.equiKey(c, leftIds, rightIds))
        val extra = Expr.and(residual).map(Eval.compile(_, l.output ++ r.output))
        val candidates: R => Seq[Int] =
          if (keys.nonEmpty) {                        // HASH JOIN: only rows with equal keys ever meet
            val leftKey = keys.map(k => Eval.compile(k._1, l.output)); val rightKey = keys.map(k => Eval.compile(k._2, r.output))
            val table = right.indices.groupBy(j => rightKey.map(_(right(j))))
            row => { val key = leftKey.map(_(row)); if (key.contains(null)) Nil else table.getOrElse(key, Nil) }
          } else {                                    // NESTED LOOP: every left row meets every right row
            spark.inc("row pairs compared", left.size.toLong * right.size); _ => right.indices
          }
        val matchedRight = mutable.BitSet()
        val (noLeft, noRight) = (Vector.fill[Any](l.output.size)(null), Vector.fill[Any](r.output.size)(null))
        val joined = left.flatMap { leftRow =>
          val matches = candidates(leftRow).filter(j => extra.forall(f => f(leftRow ++ right(j)) == true))
          matchedRight ++= matches
          if (matches.isEmpty && (joinType == "left_outer" || joinType == "full_outer")) Vector(leftRow ++ noRight) else matches.map(j => leftRow ++ right(j))
        }
        if (joinType == "right_outer" || joinType == "full_outer") joined ++ right.indices.filterNot(matchedRight).map(j => noLeft ++ right(j)) else joined
      case L.Limit(n, child) => run(child).take(n)
      case L.Distinct(child) => run(child).distinct
      case o: L.Opaque => o.run(run(o.child))
      case L.UnresolvedRelation(name) => throw new IllegalStateException(s"unresolved relation $name")
    }
    run(plan)
  }
}

// ----- PHYSICAL PLANNING: HOW to run each logical operator, chosen with statistics (section 16) ---------
object Planner {
  import Plans.Node
  private def estimate(plan: LogicalPlan): Long = plan match {               // statistics: a (crude) estimate of the number of rows
    case r: L.Relation => r.partitionFilter.fold(r.data.size.toLong)(_ => math.max(1L, r.data.size / 3))
    case L.Filter(condition, child) => condition match {
      case BinOp("<", Ref(_), Lit(v: Int)) => math.min(estimate(child), v.toLong)       // (pretend we have value statistics for this simple case)
      case _ => math.max(1L, estimate(child) / 2) }
    case L.Join(l, r, _, _) => math.max(estimate(l), estimate(r))
    case L.Aggregate(_, _, child) => math.max(1L, estimate(child) / 2)
    case L.Limit(n, child) => math.min(n.toLong, estimate(child))
    case other => other.children.map(estimate).maxOption.getOrElse(1L)
  }
  private def list(exprs: Seq[Expr]): String = exprs.map(Expr.str(_, ids = true)).mkString("[", ", ", "]")
  def plan(logical: LogicalPlan, spark: SparkSession): Node = {
    def go(p: LogicalPlan): Node = p match {
      case r: L.Relation => Node("Scan " + Plans.describe(r))
      case L.SubqueryAlias(_, child) => go(child)
      case L.Project(exprs, child) => Node(s"Project ${list(exprs)}", List(go(child)))
      case L.Filter(condition, child) => Node(s"Filter ${Expr.str(condition, ids = true)}", List(go(child)))
      case L.Sort(order, child) => Node(s"Sort ${list(order)}", List(Node(s"Exchange rangepartitioning(${list(order)})   <- shuffle", List(go(child)))))
      case L.Aggregate(grouping, aggregates, child) =>
        val functions = aggregates.flatMap(e => Expr.collect(e) { case a: Agg => Expr.str(a, ids = false) })
        Node(s"HashAggregate(keys=${list(grouping)}, functions=[${functions.mkString(", ")}])   <- final: merge the partial results", List(
          Node(s"Exchange hashpartitioning(${list(grouping)})   <- shuffle: one row per key per partition", List(
            Node(s"HashAggregate(keys=${list(grouping)}, functions=[${functions.map("partial_" + _).mkString(", ")}])   <- partial: BEFORE the shuffle (map-side combine)", List(go(child)))))))
      case L.Join(l, r, joinType, condition) =>
        val (nl, nr) = (estimate(l), estimate(r))
        val keys = condition.toList.flatMap(Expr.conjuncts).flatMap(c => Expr.equiKey(c, l.output.map(_.id).toSet, r.output.map(_.id).toSet))
        val on = condition.fold("")(c => " " + Expr.str(c, ids = true))
        if (keys.isEmpty) Node(s"CartesianProduct$on   <- no equality to join on: all ~$nl x ~$nr pairs", List(go(l), go(r)))
        else if (math.min(nl, nr) <= spark.broadcastThreshold) {
          val buildRight = nr <= nl
          Node(s"BroadcastHashJoin$on, $joinType   <- cost model: one side is small (~${math.min(nl, nr)} rows): send it to every executor, the big side stays where it is",
            List(if (buildRight) go(l) else Node("BroadcastExchange", List(go(l))), if (buildRight) Node("BroadcastExchange", List(go(r))) else go(r)))
        } else Node(s"SortMergeJoin$on, $joinType   <- cost model: both sides are big (~$nl and ~$nr rows): shuffle and sort both on the join key",
          List(l, r).map(side => Node("Sort [join key]", List(Node("Exchange hashpartitioning(join key)   <- shuffle", List(go(side)))))))
      case L.Limit(n, child) => Node(s"GlobalLimit $n", List(go(child)))
      case L.Distinct(child) => Node("HashAggregate(keys=[all columns], functions=[])   <- distinct", List(Node("Exchange hashpartitioning(all columns)   <- shuffle", List(go(child)))))
      case o: L.Opaque => Node(s"${o.name}   <- black box: your JVM code on deserialized objects", List(go(o.child)))
      case L.UnresolvedRelation(name) => Node(s"'UnresolvedRelation [$name]")
    }
    go(logical)
  }
}

// ----- the Dataset: a logical plan + an encoder. DataFrame is just Dataset[Row]. -------------------------
type DataFrame = Dataset[Row]

final class Dataset[T](val sparkSession: SparkSession, val plan: LogicalPlan, val encoder: Encoder[T]) {
  private def spark = sparkSession
  private def typed(newPlan: LogicalPlan): Dataset[T] = Dataset.typed(spark, newPlan, encoder)
  private def named(names: Seq[String]): Seq[Column] = names.map(functions.col)
  private def fresh(schema: StructType): Seq[Attribute] = schema.fields.map(f => Attribute(f.name, f.dataType, f.nullable, Ids.next()))

  def schema: StructType = StructType.of(plan.output)
  def columns: Seq[String] = plan.output.map(_.name)
  def printSchema(): Unit = print(schema.treeString)
  def apply(colName: String): Column = col(colName)
  def col(colName: String): Column = new Column(Analyzer.resolve(UnresolvedAttr(colName.split('.').toList), plan.output))    // BOUND: tied to THIS plan's column
  def toDF(): DataFrame = Dataset.ofRows(spark, plan)
  def toDF(colNames: String*): DataFrame = {
    require(colNames.size == plan.output.size, s"The number of columns doesn't match. Old column names (${plan.output.size}): ${columns.mkString(", ")}. New column names (${colNames.size}): ${colNames.mkString(", ")}")
    Dataset.ofRows(spark, L.Project(plan.output.zip(colNames).map { case (a, name) => if (a.name == name) Ref(a) else Alias(Ref(a), name) }, plan))
  }
  // Dataset[Row] -> Dataset[U]: columns are matched BY NAME against the fields of U (tuples: by position). Checked at run time.
  def as[U](using target: Encoder[U]): Dataset[U] = {
    val names = target.schema.fieldNames
    val positional = target.isPrimitive || (names.forall(_.startsWith("_")) && names.size == plan.output.size)
    Dataset.typed(spark, if (positional) plan else L.Project(names.map(n => UnresolvedAttr(List(n))), plan), target)
  }

  // --- dynamically typed (Column based) transformations: they extend the logical plan with a declarative operator ---
  def select(cols: Column*): DataFrame = {
    val exprs = cols.map(_.expr)
    Dataset.ofRows(spark, if (exprs.exists(Expr.hasAggregate)) L.Aggregate(Nil, exprs, plan) else L.Project(exprs, plan))
  }
  def select(col: String, cols: String*): DataFrame = select(named(col +: cols)*)
  def filter(condition: Column): Dataset[T] = typed(L.Filter(condition.expr, plan))
  def filter(conditionExpr: String): Dataset[T] = filter(new Column(SqlParser.parseExpression(conditionExpr)))
  def where(condition: Column): Dataset[T] = filter(condition)
  def where(conditionExpr: String): Dataset[T] = filter(conditionExpr)
  def orderBy(sortExprs: Column*): Dataset[T] = typed(L.Sort(sortExprs.map(_.expr match { case order: SortOrder => order; case e => SortOrder(e, true) }), plan))
  def orderBy(sortCol: String, sortCols: String*): Dataset[T] = orderBy(named(sortCol +: sortCols)*)
  def sort(sortExprs: Column*): Dataset[T] = orderBy(sortExprs*)
  def sort(sortCol: String, sortCols: String*): Dataset[T] = orderBy(named(sortCol +: sortCols)*)
  def groupBy(cols: Column*): RelationalGroupedDataset = new RelationalGroupedDataset(this, cols.map(_.expr))
  def groupBy(col1: String, cols: String*): RelationalGroupedDataset = groupBy(named(col1 +: cols)*)
  def join(right: Dataset[?]): DataFrame = Dataset.ofRows(spark, L.Join(plan, right.plan, "cross", None))
  def join(right: Dataset[?], joinExprs: Column): DataFrame = join(right, joinExprs, "inner")
  def join(right: Dataset[?], joinExprs: Column, joinType: String): DataFrame = {
    val normalized = joinType.toLowerCase match { case "left" | "leftouter" => "left_outer"; case "right" | "rightouter" => "right_outer"; case "outer" | "full" | "fullouter" => "full_outer"; case other => other }
    Dataset.ofRows(spark, L.Join(plan, right.plan, normalized, Some(joinExprs.expr)))
  }
  def limit(n: Int): Dataset[T] = typed(L.Limit(n, plan))
  def distinct(): Dataset[T] = typed(L.Distinct(plan))

  // --- statically typed (lambda based) transformations: your function goes into the plan as an OPAQUE node ---
  def filter(func: T => Boolean): Dataset[T] = typed(L.Opaque("TypedFilter <lambda>", plan.output, plan, rows => rows.filter(row => func(encoder.fromRow(row)))))
  def map[U](func: T => U)(using target: Encoder[U]): Dataset[U] =
    Dataset.typed(spark, L.Opaque("MapElements <lambda>", fresh(target.schema), plan, rows => rows.map(row => target.toRow(func(encoder.fromRow(row))))), target)
  def flatMap[U](func: T => IterableOnce[U])(using target: Encoder[U]): Dataset[U] =
    Dataset.typed(spark, L.Opaque("FlatMapElements <lambda>", fresh(target.schema), plan, rows => rows.flatMap(row => func(encoder.fromRow(row)).iterator.map(target.toRow))), target)
  def groupByKey[K](func: T => K)(using keyEncoder: Encoder[K]): KeyValueGroupedDataset[K, T] =
    new KeyValueGroupedDataset[K, T](spark, plan, row => { val element = encoder.fromRow(row); (func(element), element) }, keyEncoder, encoder)

  // --- actions: optimize the plan, execute it, bring the result to the driver ---
  def optimizedPlan: LogicalPlan = if (spark.optimizerEnabled) Optimizer.optimize(plan) else plan
  def optimizedPlanString: String = Plans.tree(optimizedPlan).replaceAll("#\\d+", "")
  private[this] def execute(p: LogicalPlan): Vector[Vector[Any]] = Engine.execute(if (spark.optimizerEnabled) Optimizer.optimize(p) else p, spark)
  def collect()(using ClassTag[T]): Array[T] = execute(plan).map(encoder.fromRow).toArray
  def count(): Long = execute(L.Aggregate(Nil, Seq(Alias(Agg("count", Star), "count")), plan)).head.head.asInstanceOf[Long]
  def take(n: Int)(using ClassTag[T]): Array[T] = execute(L.Limit(n, plan)).map(encoder.fromRow).toArray
  def first(): T = execute(L.Limit(1, plan)).headOption.map(encoder.fromRow).getOrElse(throw new NoSuchElementException("head of empty Dataset"))
  def head(): T = first()
  def foreach(f: T => Unit): Unit = execute(plan).foreach(row => f(encoder.fromRow(row)))
  def reduce(func: (T, T) => T): T = execute(plan).map(encoder.fromRow).reduce(func)
  def show(numRows: Int = 20): Unit = {
    def cell(v: Any): String = v match {
      case null => "NULL"
      case m: Map[?, ?] => m.map { case (k, x) => s"${cell(k)} -> ${cell(x)}" }.mkString("{", ", ", "}")
      case a: Array[?] => a.map(cell).mkString("[", ", ", "]")
      case p: Product => p.productIterator.map(cell).mkString("{", ", ", "}")
      case other => val text = other.toString; if (text.length > 20) text.take(17) + "..." else text
    }
    val rows = execute(L.Limit(numRows + 1, plan))
    val table = columns +: rows.take(numRows).map(_.map(cell))
    val widths = columns.indices.map(i => math.max(3, table.map(_(i).length).max))
    val line = widths.map("-" * _).mkString("+", "+", "+")
    def format(cells: Seq[String]): String = cells.zip(widths).map { case (text, width) => " " * (width - text.length) + text }.mkString("|", "|", "|")
    println(line); println(format(table.head)); println(line); table.tail.foreach(r => println(format(r))); println(line)
    if (rows.size > numRows) println(s"only showing top $numRows row${if (numRows == 1) "" else "s"}")
  }
  def explain(extended: Boolean = false): Unit = {
    if (extended) println("== Analyzed Logical Plan ==\n" + Plans.tree(plan) + "\n\n== Optimized Logical Plan ==\n" + Plans.tree(optimizedPlan) + "\n")
    println("== Physical Plan ==\n" + Plans.render(Planner.plan(optimizedPlan, spark)).mkString("\n"))
  }
  def createOrReplaceTempView(viewName: String): Unit = spark.views(viewName) = plan
  def rdd: RDD[T] = new RDD[T](() => execute(plan).iterator.map(encoder.fromRow), spark)
}
object Dataset {
  // creating a Dataset = analyzing its plan. This is where a wrong column name blows up (at run time).
  def ofRows(spark: SparkSession, plan: LogicalPlan): DataFrame = {
    val analyzed = Analyzer.analyze(plan, spark.views.get)
    new Dataset[Row](spark, analyzed, new RowEncoder(StructType.of(analyzed.output)))
  }
  def typed[T](spark: SparkSession, plan: LogicalPlan, encoder: Encoder[T]): Dataset[T] = new Dataset[T](spark, Analyzer.analyze(plan, spark.views.get), encoder)
}

// what df.groupBy(...) returns: a grouping WITHOUT results. Only an aggregation makes it a DataFrame again.
class RelationalGroupedDataset(df: Dataset[?], grouping: Seq[Expr]) {
  private def aggregated(aggregates: Seq[Expr]): DataFrame = Dataset.ofRows(df.sparkSession, L.Aggregate(grouping, grouping ++ aggregates, df.plan))
  private def each(function: String, colNames: Seq[String]): DataFrame = aggregated(colNames.map(n => Agg(function, UnresolvedAttr(List(n)))))
  def agg(expr: Column, exprs: Column*): DataFrame = aggregated((expr +: exprs).map(_.expr))
  def count(): DataFrame = aggregated(Seq(Alias(Agg("count", Star), "count")))
  def sum(colNames: String*): DataFrame = each("sum", colNames);  def avg(colNames: String*): DataFrame = each("avg", colNames)
  def mean(colNames: String*): DataFrame = each("avg", colNames); def max(colNames: String*): DataFrame = each("max", colNames)
  def min(colNames: String*): DataFrame = each("min", colNames)
}

// what ds.groupByKey(f) returns. `split` turns a row into (key, value) by running the user's key function.
class KeyValueGroupedDataset[K, V](spark: SparkSession, plan: LogicalPlan, split: Vector[Any] => (K, V), keyEncoder: Encoder[K], valueEncoder: Encoder[V]) {
  private def fresh(schema: StructType): Seq[Attribute] = schema.fields.map(f => Attribute(f.name, f.dataType, f.nullable, Ids.next()))
  private def groups(rows: Vector[Vector[Any]]): Vector[(K, Vector[V])] = {
    val grouped = mutable.LinkedHashMap[K, mutable.ArrayBuffer[V]]()
    rows.foreach { row => val (key, value) = split(row); grouped.getOrElseUpdate(key, mutable.ArrayBuffer[V]()) += value }
    grouped.iterator.map { case (key, values) => (key, values.toVector) }.toVector
  }
  private def opaque[U](name: String, target: Encoder[U])(run: Vector[(K, Vector[V])] => Vector[U]): Dataset[U] =
    Dataset.typed(spark, L.Opaque(name, fresh(target.schema), plan, rows => run(groups(rows)).map(target.toRow)), target)

  // declarative aggregation: the key lambda only ADDS a key column; the aggregation itself stays visible to Catalyst
  def agg[U](col: TypedColumn[V, U]): Dataset[(K, U)] = {
    val key = Attribute("key", keyEncoder.cellType, true, Ids.next())
    val withKey = L.Opaque("AppendColumns <key lambda>", plan.output :+ key, plan, rows => rows.map(row => row :+ split(row)._1))
    Dataset.typed(spark, L.Aggregate(Seq(Ref(key)), Seq(Ref(key), col.expr), withKey), Encoder.tuple(keyEncoder, col.encoder, "key", Expr.str(col.expr, ids = false)))
  }
  def count(): Dataset[(K, Long)] = agg(new TypedColumn[V, Long](Agg("count", Star), summon[Encoder[Long]]))
  def mapValues[W](func: V => W)(using target: Encoder[W]): KeyValueGroupedDataset[K, W] =
    new KeyValueGroupedDataset[K, W](spark, plan, row => { val (key, value) = split(row); (key, func(value)) }, keyEncoder, target)
  def mapGroups[U](f: (K, Iterator[V]) => U)(using target: Encoder[U]): Dataset[U] =
    opaque("MapGroups <lambda>", target)(_.map { case (key, values) => f(key, values.iterator) })
  def flatMapGroups[U](f: (K, Iterator[V]) => IterableOnce[U])(using target: Encoder[U]): Dataset[U] =
    opaque("FlatMapGroups <lambda>", target)(_.flatMap { case (key, values) => f(key, values.iterator) })
  def reduceGroups(f: (V, V) => V): Dataset[(K, V)] =
    opaque("ReduceGroups <lambda>", Encoder.tuple(keyEncoder, valueEncoder, "key", "value"))(_.map { case (key, values) => (key, values.reduce(f)) })
}

// ----- the SQL front-end: text -> the same (unresolved) logical plans the DataFrame API builds ----------
object SqlParser {
  private val keywords = Set("SELECT", "DISTINCT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER", "LIMIT", "JOIN", "INNER", "LEFT", "RIGHT", "FULL",
    "OUTER", "CROSS", "ON", "AS", "AND", "OR", "NOT", "ASC", "DESC", "IS", "NULL", "TRUE", "FALSE", "CAST", "ALL")       // reserved: not usable as names
  private val token = """\s*(\d+\.\d+|\d+|'[^']*'|[A-Za-z_][A-Za-z0-9_]*|<=|>=|<>|!=|[=<>(),.*+\-/])""".r
  def tokenize(text: String): Vector[String] = {
    val tokens = Vector.newBuilder[String]; var rest = text
    while (rest.trim.nonEmpty) token.findPrefixMatchOf(rest) match {
      case Some(m) => tokens += m.group(1); rest = rest.substring(m.end)
      case None    => throw new ParseException(s"[PARSE_SYNTAX_ERROR] Syntax error at or near '${rest.trim.take(10)}'.")
    }
    tokens.result()
  }
  def parse(sql: String): LogicalPlan = { val parser = new Parser(tokenize(sql)); val plan = parser.query(); parser.end(); plan }
  def parseExpression(text: String): Expr = { val parser = new Parser(tokenize(text)); val e = parser.expression(); parser.end(); e }
  def dataType(name: String): DataType = name.toUpperCase match {
    case "STRING" => StringType; case "INT" | "INTEGER" => IntegerType; case "LONG" | "BIGINT" => LongType
    case "DOUBLE" | "FLOAT" => DoubleType; case "BOOLEAN" => BooleanType
    case other => throw new ParseException(s"[UNSUPPORTED_DATATYPE] Unsupported data type \"$other\".")
  }
  def schema(ddl: String): StructType = StructType(ddl.split(",").toSeq.map(_.trim.split("\\s+")).map(parts => StructField(parts(0), dataType(parts(1)), true)))

  private class Parser(tokens: Vector[String]) {
    private var position = 0
    private def peek: String = if (position < tokens.size) tokens(position) else "end of input"
    private def next(): String = { val t = peek; position += 1; t }
    private def at(keyword: String): Boolean = peek.equalsIgnoreCase(keyword)
    private def accept(keyword: String): Boolean = if (at(keyword)) { position += 1; true } else false
    private def fail(): Nothing = throw new ParseException(s"[PARSE_SYNTAX_ERROR] Syntax error at or near '$peek'.")
    private def expect(keyword: String): Unit = if (!accept(keyword)) fail()
    private def isName(t: String): Boolean = (t.head.isLetter || t.head == '_') && !keywords(t.toUpperCase) && position < tokens.size
    private def name(): String = if (isName(peek)) next() else fail()
    private def list[A](item: () => A): List[A] = { val first = item(); if (accept(",")) first :: list(item) else List(first) }
    def end(): Unit = if (position < tokens.size) fail()

    def query(): LogicalPlan = {
      expect("SELECT"); val distinct = accept("DISTINCT")
      val items = list(() => selectItem())
      expect("FROM")
      var plan = joins(source())
      if (accept("WHERE")) plan = L.Filter(expression(), plan)
      val grouping = if (accept("GROUP")) { expect("BY"); list(() => expression()) } else Nil
      plan = if (grouping.nonEmpty || items.exists(Expr.hasAggregate)) L.Aggregate(grouping, items, plan) else L.Project(items, plan)
      if (accept("HAVING")) plan = L.Filter(expression(), plan)
      if (distinct) plan = L.Distinct(plan)
      if (accept("ORDER")) { expect("BY"); plan = L.Sort(list(() => orderItem()), plan) }
      if (accept("LIMIT")) plan = L.Limit(next().toInt, plan)
      plan
    }
    private def source(): LogicalPlan =
      if (accept("(")) { val subquery = query(); expect(")"); accept("AS"); L.SubqueryAlias(name(), subquery) }
      else { val table = name(); val alias = if (accept("AS") || isName(peek)) name() else table; L.SubqueryAlias(alias, L.UnresolvedRelation(table)) }
    private def joins(left: LogicalPlan): LogicalPlan = {
      val joinType =
        if (accept("JOIN")) Some("inner") else if (accept("INNER")) { expect("JOIN"); Some("inner") } else if (accept("CROSS")) { expect("JOIN"); Some("cross") }
        else if (at("LEFT") || at("RIGHT") || at("FULL")) { val side = next().toLowerCase; accept("OUTER"); expect("JOIN"); Some(side + "_outer") } else None
      joinType.fold(left) { kind =>
        val right = source(); val condition = if (accept("ON")) Some(expression()) else None
        joins(L.Join(left, right, if (condition.isEmpty && kind == "inner") "cross" else kind, condition))
      }
    }
    private def selectItem(): Expr = if (accept("*")) Star else { val e = expression(); if (accept("AS") || isName(peek)) Alias(e, name()) else e }
    private def orderItem(): SortOrder = { val e = expression(); if (accept("DESC")) SortOrder(e, false) else { accept("ASC"); SortOrder(e, true) } }
    def expression(): Expr = { var e = conjunction(); while (accept("OR")) e = BinOp("OR", e, conjunction()); e }
    private def conjunction(): Expr = { var e = negation(); while (accept("AND")) e = BinOp("AND", e, negation()); e }
    private def negation(): Expr = if (accept("NOT")) Not(negation()) else comparison()
    private def comparison(): Expr = {
      val left = sum()
      if (accept("IS")) { val negated = accept("NOT"); expect("NULL"); val test = Func("isnull", Seq(left)); if (negated) Not(test) else test }
      else peek match {
        case "=" | "<" | "<=" | ">" | ">=" => val op = next(); BinOp(op, left, sum())
        case "<>" | "!="                   => next(); BinOp("!=", left, sum())
        case _                             => left
      }
    }
    private def sum(): Expr = { var e = product(); while (peek == "+" || peek == "-") { val op = next(); e = BinOp(op, e, product()) }; e }
    private def product(): Expr = { var e = primary(); while (peek == "*" || peek == "/") { val op = next(); e = BinOp(op, e, primary()) }; e }
    private def primary(): Expr = {
      val t = peek
      if (position >= tokens.size) fail()
      else if (t.head.isDigit) { next(); if (t.contains(".")) Lit(t.toDouble) else if (t.length < 10) Lit(t.toInt) else Lit(t.toLong) }
      else if (t.head == '\'') { next(); Lit(t.substring(1, t.length - 1)) }
      else if (accept("(")) { val e = expression(); expect(")"); e }
      else if (accept("-")) BinOp("-", Lit(0), primary())
      else if (accept("TRUE")) Lit(true) else if (accept("FALSE")) Lit(false) else if (accept("NULL")) Lit(null)
      else if (accept("CAST")) { expect("("); val e = expression(); expect("AS"); val to = dataType(next()); expect(")"); Cast(e, to) }
      else if (isName(t)) {
        val identifier = next()
        if (accept("(")) {                                               // a function call: aggregate or scalar
          val function = identifier.toLowerCase
          val args = if (accept("*")) List(Star) else if (at(")")) Nil else list(() => expression())
          expect(")")
          if (Set("sum", "avg", "mean", "count", "min", "max")(function)) Agg(if (function == "mean") "avg" else function, args.head) else Func(function, args)
        }
        else if (accept(".")) UnresolvedAttr(List(identifier, name()))    // qualified: d.id
        else UnresolvedAttr(List(identifier))
      }
      else fail()
    }
  }
}

// ----- a minimal RDD for this chapter (the real thing, with partitions, lineage and shuffles, is in the chapter 2 file) ----
class RDD[T](compute: () => Iterator[T], val spark: SparkSession) {
  private var persisted = false; private var cached: Option[Vector[T]] = None
  def iterator: Iterator[T] = if (!persisted) compute() else { if (cached.isEmpty) cached = Some(compute().toVector); cached.get.iterator }
  def persist(): this.type = { persisted = true; this }
  def map[U](f: T => U): RDD[U] = new RDD(() => iterator.map(f), spark)
  def flatMap[U](f: T => IterableOnce[U]): RDD[U] = new RDD(() => iterator.flatMap(f), spark)
  def filter(f: T => Boolean): RDD[T] = new RDD(() => iterator.filter(f), spark)
  def cartesian[U](other: RDD[U]): RDD[(T, U)] = new RDD(() => { val right = other.iterator.toVector; iterator.flatMap(l => right.iterator.map { r => spark.inc("pairs built by cartesian"); (l, r) }) }, spark)
  def count(): Long = iterator.size.toLong
  def collect()(using ClassTag[T]): Array[T] = iterator.toArray
}
extension [K, V](self: RDD[(K, V)]) {
  def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = new RDD(() => {
    val (left, right) = (self.iterator.toVector, other.iterator.toVector)
    self.spark.inc("records shuffled", left.size + right.size)           // a join shuffles BOTH inputs, completely
    val byKey = right.groupMap(_._1)(_._2)
    left.iterator.flatMap { case (k, v) => byKey.getOrElse(k, Nil).iterator.map(w => (k, (v, w))) }
  }, self.spark)
  def groupByKey(): RDD[(K, Iterable[V])] = new RDD(() => { val all = self.iterator.toVector; self.spark.inc("records shuffled", all.size); all.groupMap(_._1)(_._2).iterator.map { case (k, vs) => (k, vs: Iterable[V]) } }, self.spark)
  def reduceByKey(f: (V, V) => V): RDD[(K, V)] = new RDD(() => {       // 4 pretend-partitions: reduce inside each one first (map-side combine)
    val all = self.iterator.toVector
    val partials = all.grouped(math.max(1, all.size / 4)).flatMap(_.groupMapReduce(_._1)(_._2)(f)).toVector
    self.spark.inc("records shuffled", partials.size)
    partials.groupMapReduce(_._1)(_._2)(f).iterator }, self.spark)
  def mapValues[U](f: V => U): RDD[(K, U)] = self.map { case (k, v) => (k, f(v)) }
}
class SparkContext(spark: SparkSession) {
  def parallelize[T](seq: Seq[T]): RDD[T] = new RDD(() => seq.iterator, spark)
  def textFile(path: String): RDD[String] = new RDD(() => spark.fs.read(path).iterator, spark)
}

// ----- the SparkSession: the entry point of Spark SQL (it contains the SparkContext) ---------------------
class SparkSession { session =>
  Ids.reset(); SQLConf.ansiEnabled = true
  val sparkContext = new SparkContext(this)
  val views = mutable.Map[String, LogicalPlan]()                     // the catalog of temp views: name -> plan
  var optimizerEnabled = true
  var broadcastThreshold = 500L                                      // rows (real Spark: spark.sql.autoBroadcastJoinThreshold = 10 MB)
  object conf { def set(key: String, value: String): Unit = if (key == "spark.sql.ansi.enabled") SQLConf.ansiEnabled = value.toBoolean }

  // a fake file system: path -> lines
  object fs {
    val files = mutable.LinkedHashMap[String, Vector[String]]()
    def write(path: String, lines: Seq[String]): Unit = files(path) = lines.toVector
    def read(path: String): Vector[String] = files.getOrElse(path, throw new AnalysisException(s"[PATH_NOT_FOUND] Path does not exist: $path."))
    // a directory with sub-directories "column=value" is partitioned data: returns (partition value, line) pairs
    def partitions(path: String): Option[(String, Vector[(String, String)])] = {
      val inside = files.keys.filter(_.startsWith(path + "/")).toVector.map(file => (file.stripPrefix(path + "/").split("/").head, file)).filter(_._1.contains("="))
      if (inside.isEmpty) None else Some((inside.head._1.split("=")(0), inside.flatMap { case (directory, file) => files(file).map(line => (directory.split("=")(1), line)) }))
    }
  }

  private def relation(name: String, schema: StructType, rows: Vector[Vector[Any]], partitionedBy: Option[String] = None): LogicalPlan = {
    val output = schema.fields.map(f => Attribute(f.name, f.dataType, f.nullable, Ids.next()))
    L.Relation(name, output, rows, output, partitionedBy)
  }
  def createDataset[T](data: Seq[T], name: String = "LocalRelation")(using encoder: Encoder[T]): Dataset[T] =
    new Dataset[T](this, relation(name, encoder.schema, data.iterator.map(encoder.toRow).toVector), encoder)
  def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame = Dataset.ofRows(this, relation("LogicalRDD", schema, rowRDD.iterator.map(_.values).toVector))
  def sql(sqlText: String): DataFrame = Dataset.ofRows(this, SqlParser.parse(sqlText))
  def read: DataFrameReader = new DataFrameReader

  class DataFrameReader {
    private var options = Map[String, String](); private var userSchema: Option[StructType] = None
    def option(key: String, value: String): DataFrameReader = { options += key -> value; this }
    def schema(ddl: String): DataFrameReader = { userSchema = Some(SqlParser.schema(ddl)); this }
    def schema(structType: StructType): DataFrameReader = { userSchema = Some(structType); this }
    // JSON is self-describing: scan the records, collect all field names (sorted) and infer their types
    def json(path: String): DataFrame = {
      val field = """"(\w+)"\s*:\s*("(?:[^"\\]|\\.)*"|-?\d+\.\d+|-?\d+|true|false|null)""".r
      def parseValue(text: String): Any = if (text.startsWith("\"")) text.substring(1, text.length - 1) else if (text == "true" || text == "false") text.toBoolean
        else if (text == "null") null else if (text.contains(".")) text.toDouble else text.toLong
      val records = fs.read(path).map(line => field.findAllMatchIn(line).map(m => m.group(1) -> parseValue(m.group(2))).toMap)
      val fields = records.flatMap(_.keys).distinct.sorted.map { name =>
        val dataType = records.flatMap(_.get(name)).filter(_ != null).map { case _: String => StringType; case _: Boolean => BooleanType; case _: Long => LongType; case _ => DoubleType }
          .distinct match { case Seq(one) => one; case Seq(LongType, DoubleType) | Seq(DoubleType, LongType) => DoubleType; case _ => StringType }
        StructField(name, dataType, nullable = true)
      }
      Dataset.ofRows(session, relation(s"Relation json", StructType(fields), records.map(record => fields.map(f => record.getOrElse(f.name, null)).toVector)))
    }
    // CSV carries no types: all strings (_c0, _c1, ...) unless you provide a schema
    def csv(path: String): DataFrame = {
      val delimiter = java.util.regex.Pattern.quote(options.getOrElse("delimiter", ","))
      val (partitionColumn, lines) = fs.partitions(path) match { case Some((column, tagged)) => (Some(column), tagged); case None => (None, fs.read(path).map(line => ("", line))) }
      val width = lines.headOption.fold(0)(_._2.split(delimiter).length)
      val dataSchema = userSchema.getOrElse(StructType((0 until width).map(i => StructField(s"_c$i", StringType))))
      val rows = lines.map { case (partitionValue, line) =>
        val cells = line.split(delimiter).zip(dataSchema.fields).map { case (text, f) => Eval.cast(text, f.dataType) }.toVector
        if (partitionColumn.isDefined) cells :+ partitionValue else cells
      }
      val fullSchema = partitionColumn.fold(dataSchema)(column => StructType(dataSchema.fields :+ StructField(column, StringType)))
      Dataset.ofRows(session, relation(s"Relation csv", fullSchema, rows, partitionColumn))
    }
  }

  // real Spark: import spark.implicits._   brings in $"..." and toDF / toDS
  object implicits {
    extension (context: StringContext) def $(args: Any*): Column = functions.col(context.s(args*))
    extension [T](data: Seq[T])(using encoder: Encoder[T]) {
      def toDS(): Dataset[T] = createDataset(data)
      def toDF(colNames: String*): DataFrame = if (colNames.isEmpty) createDataset(data).toDF() else createDataset(data).toDF(colNames*)
    }
    extension [T](rdd: RDD[T])(using encoder: Encoder[T]) {
      def toDS(): Dataset[T] = createDataset(rdd.iterator.toSeq, "LogicalRDD")
      def toDF(colNames: String*): DataFrame = if (colNames.isEmpty) createDataset(rdd.iterator.toSeq, "LogicalRDD").toDF() else createDataset(rdd.iterator.toSeq, "LogicalRDD").toDF(colNames*)
    }
  }

  // counters for the demos
  private val counters = mutable.LinkedHashMap[String, Long]()
  List("rows scanned", "values read", "partitions read", "rows entering joins", "row pairs compared", "records shuffled", "pairs built by cartesian").foreach(counters(_) = 0L)
  def inc(name: String, by: Long = 1L): Unit = counters(name) = counters(name) + by
  def measure[A](label: String)(body: => A): A = {
    val before = counters.toMap; val start = System.nanoTime()
    val result = body
    val millis = (System.nanoTime() - start) / 1000000
    val work = counters.toList.map { case (name, value) => (name, value - before(name)) }.filter(_._2 != 0)
    println(s"   >> [$label] " + (if (work.isEmpty) "nothing was executed" else work.map { case (name, n) => s"$name=$n" }.mkString(", ") + s"   ($millis ms)"))
    result
  }
  // every phase of the Spark SQL pipeline for one SQL query
  def explainPipeline(sqlText: String): Unit = {
    println(s"   SQL text: $sqlText")
    val parsed = SqlParser.parse(sqlText)
    println("   1. PARSED: an unresolved logical plan. 'name = just a name, nobody has checked that it exists\n" + indent(Plans.tree(parsed)))
    val analyzed = Analyzer.analyze(parsed, views.get)
    println("   2. ANALYZED with the catalog + schema: names are now concrete columns with ids (#) and types\n" + indent(Plans.tree(analyzed)))
    val optimized = Optimizer.optimize(analyzed)
    println("   3. OPTIMIZED logical plan: same result, less work\n" + indent(Plans.tree(optimized)))
    println("   4. PHYSICAL plan, selected with the cost model; real Spark then generates code and runs it as RDDs\n" + indent(Plans.render(Planner.plan(optimized, this)).mkString("\n")))
  }
  private def indent(text: String): String = text.linesIterator.map("        " + _).mkString("\n")
}
