import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object BatchProcessor {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // 1. CREATE SPARK SESSION
    // ============================================================

    val spark = SparkSession.builder()
      .appName("Invoice Processing Pipeline")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ============================================================
    // 2. ACCUMULATOR
    // ============================================================

    val badRecordAccumulator =
      spark.sparkContext.longAccumulator("Bad Invoice Records")

    // ============================================================
    // 3. READ INVOICE DATA
    // ============================================================

    val invoiceDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/input/invoices.csv")

    println("===== ORIGINAL INVOICES =====")
    invoiceDF.show()

    invoiceDF.printSchema()

    // ============================================================
    // 4. UDF FOR INVOICE VALIDATION
    // ============================================================

    val validateInvoice = udf(
      (amount: Int, tax: Int, totalAmount: Int) => {
        amount >= 0 && (amount + tax == totalAmount)
      }
    )

    // ============================================================
    // 5. VALIDATE INVOICES + CACHE
    // ============================================================

    val validatedDF = invoiceDF
      .withColumn(
        "is_valid",
        validateInvoice(
          col("amount"),
          col("tax"),
          col("total_amount")
        )
      )
      .persist(StorageLevel.MEMORY_AND_DISK)

    // Materialize cache
    validatedDF.count()

    println("===== VALIDATED INVOICES =====")
    validatedDF.show()

    // ============================================================
    // 6. SEPARATE VALID AND INVALID INVOICES
    // ============================================================

    val validInvoices = validatedDF
      .filter(col("is_valid") === true)

    println("===== VALID INVOICES =====")
    validInvoices.show()

    val invalidInvoices = validatedDF
      .filter(col("is_valid") === false)

    println("===== INVALID INVOICES =====")
    invalidInvoices.show()

    // ============================================================
    // 7. ACCUMULATOR - COUNT BAD RECORDS
    // ============================================================

    // Only select invoice_id to avoid unnecessary type conversion.
    invalidInvoices
      .select("invoice_id")
      .rdd
      .foreach { row =>
        badRecordAccumulator.add(1)
      }

    println(
      s"Bad records found: ${badRecordAccumulator.value}"
    )

    // ============================================================
    // 8. WINDOW FUNCTION - DUPLICATE DETECTION
    // ============================================================

    val windowSpec = Window
      .partitionBy("invoice_id")
      .orderBy(col("invoice_date").desc)

    val withRowNumberDF = validInvoices
      .withColumn(
        "row_number",
        row_number().over(windowSpec)
      )

    println("===== WITH ROW NUMBERS =====")
    withRowNumberDF.show()

    // ============================================================
    // 9. KEEP ONLY FIRST RECORD
    // ============================================================

    val uniqueInvoices = withRowNumberDF
      .filter(col("row_number") === 1)
      .drop("row_number")

    println("===== UNIQUE VALID INVOICES =====")
    uniqueInvoices.show()

    println(
      s"Unique valid invoices: ${uniqueInvoices.count()}"
    )

    // ============================================================
    // 10. READ VENDOR REFERENCE DATA
    // ============================================================

    val vendorDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/reference/vendors.csv")

    println("===== VENDOR REFERENCE DATA =====")
    vendorDF.show()

    vendorDF.printSchema()

    // ============================================================
    // 11. SELECT REQUIRED REFERENCE COLUMNS
    // ============================================================

    val vendorReferenceDF = vendorDF.select(
      col("vendor_id"),
      col("vendor_category")
    )

    // ============================================================
    // 12. BROADCAST JOIN
    // ============================================================

    val enrichedInvoices = uniqueInvoices
      .join(
        broadcast(vendorReferenceDF),
        Seq("vendor_id"),
        "left"
      )

    println("===== ENRICHED INVOICES =====")
    enrichedInvoices.show()

    // ============================================================
    // 13. FILTER PAYABLE INVOICES
    // ============================================================

    val payableInvoices = enrichedInvoices
      .filter(col("status") === "PENDING")

    println("===== PAYABLE INVOICES =====")
    payableInvoices.show()

    // ============================================================
    // 14. VENDOR LEVEL AGGREGATION
    // ============================================================

    val vendorPayable = payableInvoices
      .groupBy(
        "vendor_id",
        "vendor_name",
        "vendor_category"
      )
      .agg(
        sum("total_amount").alias("total_payable"),
        count("*").alias("invoice_count")
      )
      .orderBy(col("total_payable").desc)

    println("===== VENDOR PAYABLE REPORT =====")
    vendorPayable.show()

    // ============================================================
    // 15. PAIR RDD OPERATIONS
    // ============================================================

    println("===== PAIR RDD OPERATIONS =====")

    /*
     * Demonstrate Pair RDD without reduceByKey.
     *
     * reduceByKey is a wide transformation and causes a shuffle.
     * The vendor-level aggregation above already demonstrates
     * the required wide aggregation in the DataFrame API.
     *
     * mapValues is a Pair RDD transformation that does not
     * require a shuffle.
     */

    val vendorPairRDD =
      payableInvoices.select(
        "vendor_id",
        "total_amount"
      ).rdd.map { row =>

        val vendorId =
          row.getAs[String]("vendor_id")

        val amount =
          row.getAs[Int]("total_amount").toDouble

        (vendorId, amount)
      }

    val pairRDDResult =
      vendorPairRDD.mapValues(amount => amount)

    println("Pair RDD records:")

    pairRDDResult.collect().foreach {
      case (vendor, amount) =>
        println(s"$vendor -> $amount")
    }

    println("Pair RDD operations completed successfully.")

    // ============================================================
    // 16. REPARTITION
    // ============================================================

    println("===== REPARTITION / COALESCE =====")

    println(
      s"Original partitions: ${uniqueInvoices.rdd.getNumPartitions}"
    )

    val repartitionedDF =
      uniqueInvoices.repartition(4)

    println(
      s"After repartition(4): ${repartitionedDF.rdd.getNumPartitions}"
    )

    // ============================================================
    // 17. COALESCE
    // ============================================================

    val coalescedDF =
      repartitionedDF.coalesce(2)

    println(
      s"After coalesce(2): ${coalescedDF.rdd.getNumPartitions}"
    )

    // ============================================================
    // 18. SPARK SQL
    // ============================================================

    println("===== SPARK SQL RESULT =====")

    enrichedInvoices.createOrReplaceTempView("invoices")

    val sqlResult = spark.sql(
      """
        SELECT
          vendor_id,
          vendor_name,
          vendor_category,
          SUM(total_amount) AS total_payable,
          COUNT(*) AS invoice_count
        FROM invoices
        WHERE status = 'PENDING'
        GROUP BY
          vendor_id,
          vendor_name,
          vendor_category
        ORDER BY total_payable DESC
      """
    )

    sqlResult.show()

    // ============================================================
    // 19. WRITE OUTPUT FILES
    // ============================================================

    println("===== WRITING OUTPUT FILES =====")

    validInvoices
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/output/valid_invoices")

    invalidInvoices
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/output/invalid_invoices")

    vendorPayable
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/output/vendor_payable")

    println("Output files written successfully.")

    // ============================================================
    // 20. CLEAN UP CACHE
    // ============================================================

    validatedDF.unpersist()

    // ============================================================
    // 21. STOP SPARK
    // ============================================================

    spark.stop()

    println("===== INVOICE PROCESSING COMPLETED =====")
  }
}
