package enterate

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

import java.io.File
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.format.DateTimeFormatter
import scala.util.control.NonFatal
import scala.collection.JavaConverters._

// Jackson para construir JSON válido
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode

object StreamingJob {

  private val isoUtc: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT
  private def toIsoUtc(ts: java.sql.Timestamp): String = isoUtc.format(ts.toInstant)

  private def optStr(a: Any): String = Option(a).map(_.toString).getOrElse("")

  def main(args: Array[String]): Unit = {

    System.setProperty("SPARK_LOCAL_HOSTNAME", "127.0.0.1")

    val spark = SparkSession.builder()
      .appName("Enterate-Streaming-Local")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.session.timeZone", "America/Santo_Domingo")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ========= Config =========
    val KAFKA_BOOTSTRAP = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
    val KAFKA_TOPIC     = sys.env.getOrElse("KAFKA_TOPIC", "enterate_incidents")

    val defaultOut        = new File("out/incidents_decisions").getAbsolutePath
    val defaultCheckpoint = new File("checkpoints/v5_incidents_decisions").getAbsolutePath
    val OUTPUT_DIR        = sys.env.getOrElse("OUTPUT_DIR", defaultOut)
    val CHECKPOINT_DIR    = sys.env.getOrElse("CHECKPOINT_DIR", defaultCheckpoint)

    // API real según tu Swagger: POST /incidents/
    val API_URL = sys.env.getOrElse("API_URL", "http://localhost:8001/incidents/")
    val CITY    = sys.env.getOrElse("CITY", "Madrid")

    new File(OUTPUT_DIR).mkdirs()
    new File(CHECKPOINT_DIR).mkdirs()

    println(s"[StreamingJob] Kafka = $KAFKA_BOOTSTRAP, topic = $KAFKA_TOPIC")
    println(s"[StreamingJob] OUTPUT_DIR = $OUTPUT_DIR")
    println(s"[StreamingJob] CHECKPOINT_DIR = $CHECKPOINT_DIR")
    println(s"[StreamingJob] API = $API_URL  CITY = $CITY")

    // ========= Esquema esperado del emulador =========
    val schema = StructType(Seq(
      StructField("report_id", StringType),
      StructField("report_kind", StringType),      // "new" | "end"
      StructField("incident_id", StringType),
      StructField("type", StringType),
      StructField("title", StringType),
      StructField("description", StringType),
      StructField("district", StringType),
      StructField("neighborhood", StringType),
      StructField("incident_lat", DoubleType),
      StructField("incident_lon", DoubleType),
      StructField("lat", DoubleType),
      StructField("lon", DoubleType),
      StructField("reported_at", StringType),      // ISO-8601
      StructField("status", StringType),
      StructField("ended_at", StringType),
      StructField("source", StringType),
      StructField("reporter", StructType(Seq(StructField("id", StringType))))
    ))

    // ========= 1) Kafka fuente =========
    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
      .option("subscribe", KAFKA_TOPIC)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    // ========= 2) Parseo JSON + event_ts =========
    val parsed = raw
      .selectExpr("CAST(value AS STRING) AS json", "timestamp AS kafka_ts")
      .select(from_json(col("json"), schema).as("e"), col("kafka_ts"))
      .selectExpr(
        "e.report_id", "e.report_kind", "e.incident_id", "e.type", "e.title",
        "e.description", "e.district", "e.neighborhood",
        "e.incident_lat", "e.incident_lon", "e.lat", "e.lon",
        "e.reported_at", "e.status", "e.ended_at", "e.source", "e.reporter",
        "kafka_ts"
      )
      .withColumn("event_ts", to_timestamp(col("reported_at")))

    // ========= 3) Flags + watermark =========
    val withFlags = parsed
      .withColumn("is_new", when(col("report_kind") === lit("new"), lit(1)).otherwise(lit(0)))
      .withColumn("is_end", when(col("report_kind") === lit("end"), lit(1)).otherwise(lit(0)))
      .withWatermark("event_ts", "10 minutes")

    // ========= 4) Ventana 5 min por zona + tipo =========
    val agg = withFlags
      .groupBy(
        window(col("event_ts"), "5 minutes"),
        col("district"),
        col("neighborhood"),
        col("type")
      )
      .agg(
        sum(col("is_new")).as("new_count"),
        sum(col("is_end")).as("end_count"),
        min(col("event_ts")).as("first_event_ts"),
        max(col("event_ts")).as("last_event_ts"),
        avg(col("incident_lat")).as("lat_avg"),
        avg(col("incident_lon")).as("lon_avg")
      )

    // ========= 5) Reglas =========
    val decision = agg
      .withColumn(
        "decision_reason",
        when(col("end_count") >= 1 && col("new_count") === 0, lit("resolved_only"))
          .when(col("end_count") === 0 && col("new_count") >= 2, lit("consensus_new"))
          .when(col("end_count") === 0 && col("new_count") >= 1, lit("unattended"))
          .otherwise(lit(null).cast(StringType))
      )
      .filter(col("decision_reason").isNotNull)
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("district"), col("neighborhood"), col("type"),
        col("new_count"), col("end_count"),
        col("first_event_ts"), col("last_event_ts"),
        col("lat_avg"), col("lon_avg"),
        col("decision_reason")
      )

    // ========= 6) Único sink: foreachBatch -> CSV + POST /incidents/ =========
    val query = decision.writeStream
      .outputMode("update")
      .option("checkpointLocation", CHECKPOINT_DIR)
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        if (!batch.isEmpty) {
          println(s"[foreachBatch] batchId=$batchId, rows=${batch.count()}")

          // 6.1 CSV
          batch
            .coalesce(1)
            .write
            .mode("append")
            .option("header", "true")
            .csv(OUTPUT_DIR)

          // 6.2 POST por fila usando Jackson
          val client = HttpClient.newHttpClient()
          val mapper = new ObjectMapper()

          batch.toLocalIterator().asScala.foreach { row: Row =>
            val district     = optStr(row.getAs[String]("district"))
            val neighborhood = optStr(row.getAs[String]("neighborhood"))
            val category     = optStr(row.getAs[String]("type"))      // <- category = tipo
            val reason       = optStr(row.getAs[String]("decision_reason"))
            val newCount     = row.getAs[Long]("new_count")
            val endCount     = row.getAs[Long]("end_count")
            val latAvg       = Option(row.getAs[Double]("lat_avg")).getOrElse(0.0d)
            val lonAvg       = Option(row.getAs[Double]("lon_avg")).getOrElse(0.0d)
            val wStart       = row.getAs[java.sql.Timestamp]("window_start")
            val wEnd         = row.getAs[java.sql.Timestamp]("window_end")

            val eventId = s"${district}_${neighborhood}_${category}_${wStart.getTime}"

            // === NUEVO: status de negocio
            // Si solo hay reportes de fin (regla resolved_only) => "closed", en caso contrario => "active"
            val statusBiz = if (reason == "resolved_only" || (endCount >= 1 && newCount == 0)) "closed" else "active"

            // JSON seguro con Jackson
            val root: ObjectNode = mapper.createObjectNode()
            root.put("source", "spark")
            root.put("category", category)      // <- el tipo de situación
            root.put("status", statusBiz)       // <- "active" | "closed"
            root.put("city", CITY)
            root.put("street", neighborhood)
            root.put("street_number", "")
            root.put("lat", latAvg)
            root.put("lon", lonAvg)
            root.put("start_ts_utc", toIsoUtc(wStart))
            root.put("end_ts_utc", toIsoUtc(wEnd))
            root.put(
              "description",
              s"status=$statusBiz; rule=$reason; new=$newCount; end=$endCount; district=$district; neighborhood=$neighborhood"
            )
            root.put("event_id", eventId)

            val body: String = mapper.writeValueAsString(root)

            try {
              val req = HttpRequest.newBuilder()
                .uri(URI.create(API_URL))  // p.ej. http://localhost:8001/incidents/
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build()

              val resp = client.send(req, HttpResponse.BodyHandlers.ofString())
              val code = resp.statusCode()
              if (code / 100 != 2) {
                System.err.println(s"[API] POST $API_URL -> $code; body=${resp.body()}")
              } else {
                println(s"[API] OK event_id=$eventId (${district}/${neighborhood} $category)")
              }
            } catch {
              case NonFatal(e) =>
                System.err.println(s"[API] Error POST event_id=$eventId : ${e.getMessage}")
            }
          }
        } else {
          println(s"[foreachBatch] batchId=$batchId vacío")
        }
      }
      .start()

    query.awaitTermination()
    spark.stop()
  }
}
