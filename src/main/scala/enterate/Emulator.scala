package enterate

import java.io.{BufferedReader, File, FileReader}
import java.util.Properties
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}
import java.util.concurrent.ThreadLocalRandom
import java.time.Instant
import scala.collection.concurrent.TrieMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Emulator {

  // ------------ Tipos y configuración ------------
  case class Zone(
                   district: String,
                   neighborhood: String,
                   latMin: Double, latMax: Double,
                   lonMin: Double, lonMax: Double
                 )

  case class Config(
                     citizens: Int,
                     zonesCsv: String,
                     kafkaEnabled: Boolean,
                     kafkaBootstrap: String,
                     kafkaTopic: String,
                     gapMinMinutes: Int,     // mínimo entre envíos (min)
                     gapMaxMinutes: Int,     // máximo entre envíos (min)
                     minuteMillis: Long,     // 60000 real; 1000 para pruebas rápidas
                     endProbability: Double, // prob. de enviar "end"
                     verbose: Boolean
                   )

  // ------------ Carga de zonas desde CSV ------------
  def loadZones(csvPath: String): Vector[Zone] = {
    val f = new File(csvPath)
    require(f.exists(), s"zones csv no encontrado: $csvPath")

    val zs = scala.collection.mutable.ArrayBuffer[Zone]()
    val br = new BufferedReader(new FileReader(f))
    try {
      var line: String = br.readLine()
      var ln = 0
      while (line != null) {
        val raw = line.trim
        if (!(ln == 0 && raw.toLowerCase.startsWith("distrito,")) && raw.nonEmpty) {
          val p = raw.split(",").map(_.trim)
          if (p.length >= 6) {
            zs += Zone(p(0), p(1), p(2).toDouble, p(3).toDouble, p(4).toDouble, p(5).toDouble)
          }
        }
        ln += 1
        line = br.readLine()
      }
    } finally br.close()
    zs.toVector
  }

  // ------------ Utilidades aleatorias ------------
  private def randBetween(min: Double, max: Double): Double =
    ThreadLocalRandom.current().nextDouble(min, max) // continuo

  private def pickOne[A](v: Vector[A]): A =
    v(ThreadLocalRandom.current().nextInt(v.size))

  private def jitterInBox(z: Zone): (Double, Double) = {
    val la = randBetween(z.latMin, z.latMax)
    val lo = randBetween(z.lonMin, z.lonMax)
    (la, lo)
  }

  // ------------ Catálogo de tipos de incidentes ------------
  private val incidentTypes: Vector[String] = Vector(
    "power_outage", "road_closure", "water_cut",
    "gas_leak", "noise_complaint", "tree_fall"
  )

  // Incidentes activos para coincidencia entre ciudadanos
  // clave: (district, neighborhood, type) -> incidentId activo
  private val activeIncidents = TrieMap.empty[(String,String,String), String]

  // ------------ Kafka producer ------------
  private def mkProducer(bootstrap: String): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("key.serializer"  , "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }

  // ------------ JSON para Kafka ------------
  private def buildKafkaJson(
                              reportKind: String,     // "new" | "end"
                              incidentId: String,
                              incType: String,
                              title: String,
                              description: String,
                              district: String, neighborhood: String,
                              incidentLat: Double, incidentLon: Double,
                              reporterId: String,
                              status: String
                            ): String = {
    val now = Instant.now().toString
    s"""{
      "report_id":"${java.util.UUID.randomUUID()}",
      "report_kind":"$reportKind",
      "incident_id":"$incidentId",
      "type":"$incType",
      "title":"$title",
      "description":"$description",
      "district":"$district",
      "neighborhood":"$neighborhood",
      "incident_lat":$incidentLat,
      "incident_lon":$incidentLon,
      "lat":$incidentLat,
      "lon":$incidentLon,
      "reported_at":"$now",
      "status":"$status",
      "ended_at":null,
      "source":"emulator",
      "reporter":{"id":"$reporterId"}
    }""".stripMargin
  }

  // ------------ Scheduling por ciudadano (continuo) ------------
  private def scheduleNext(
                            exec: ScheduledThreadPoolExecutor,
                            cfg: Config,
                            zones: Vector[Zone],
                            citizenId: Int,
                            producerOpt: Option[KafkaProducer[String,String]]
                          ): Unit = {
    // Delay continuo aleatorio (2..15 min por defecto)
    val delayMinD: Double =
      ThreadLocalRandom.current().nextDouble(cfg.gapMinMinutes.toDouble, cfg.gapMaxMinutes.toDouble)
    val delayMs: Long = math.max(1L, math.round(delayMinD * cfg.minuteMillis))

    exec.schedule(
      new Runnable {
        override def run(): Unit = {
          try {
            val zone = pickOne(zones)
            val (ilat, ilon) = jitterInBox(zone)
            val tpe = pickOne(incidentTypes)
            val key = (zone.district, zone.neighborhood, tpe)

            val rnd = ThreadLocalRandom.current().nextDouble()
            val (kind, incidentId) =
              if (rnd < cfg.endProbability) {
                activeIncidents.get(key) match {
                  case Some(id) => ("end", id)
                  case None =>
                    val nid = java.util.UUID.randomUUID().toString
                    activeIncidents.put(key, nid)
                    ("new", nid)
                }
              } else {
                val reuse = ThreadLocalRandom.current().nextBoolean()
                val id =
                  if (reuse) activeIncidents.getOrElseUpdate(key, java.util.UUID.randomUUID().toString)
                  else {
                    val nid = java.util.UUID.randomUUID().toString
                    activeIncidents.put(key, nid)
                    nid
                  }
                ("new", id)
              }

            val title = if (kind == "new") s"Reporte de $tpe" else s"Cierre de $tpe"
            val desc  = s"Ciudadano $citizenId ${if (kind == "new") "reporta" else "indica fin de"} $tpe"

            val json = buildKafkaJson(
              reportKind = kind,
              incidentId = incidentId,
              incType    = tpe,
              title      = title,
              description= desc,
              district   = zone.district,
              neighborhood = zone.neighborhood,
              incidentLat = ilat,
              incidentLon = ilon,
              reporterId  = f"cit-$citizenId%04d",
              status      = if (kind == "new") "open" else "ended"
            )

            if (cfg.kafkaEnabled) {
              val rec = new ProducerRecord[String,String](cfg.kafkaTopic, incidentId, json)
              producerOpt.get.send(rec)
            }
            if (cfg.verbose) {
              println(f"[citizen=$citizenId] kind=$kind type=$tpe zone=${zone.district}/${zone.neighborhood} " +
                f"@($ilat%.5f,$ilon%.5f) :: incident=$incidentId :: next ~ ${delayMinD}%.2f min")
            }

            if (kind == "end") {
              activeIncidents.remove(key) // opcional
            }

          } catch {
            case e: Throwable =>
              System.err.println(s"[citizen=$citizenId] error: ${e.getMessage}")
          } finally {
            scheduleNext(exec, cfg, zones, citizenId, producerOpt) // reprograma
          }
        }
      },
      delayMs,
      TimeUnit.MILLISECONDS
    )
  }

  // ------------ Entrada pública ------------
  def start(cfg: Config): Unit = {
    require(cfg.gapMinMinutes >= 1 && cfg.gapMaxMinutes >= cfg.gapMinMinutes,
      s"GAP_MIN=${cfg.gapMinMinutes} y GAP_MAX=${cfg.gapMaxMinutes} deben ser válidos")
    val zones = loadZones(cfg.zonesCsv)
    require(zones.nonEmpty, s"No se cargaron zonas desde ${cfg.zonesCsv}")

    val poolSize = math.min(256, math.max(8, cfg.citizens / 4))
    val exec = new ScheduledThreadPoolExecutor(poolSize)
    val producer = if (cfg.kafkaEnabled) Some(mkProducer(cfg.kafkaBootstrap)) else None

    (1 to cfg.citizens).foreach { cid =>
      scheduleNext(exec, cfg, zones, cid, producer)
    }

    println(
      s"[Emulator] ciudadanos=${cfg.citizens}, intervalo CONTINUO=${cfg.gapMinMinutes}-${cfg.gapMaxMinutes} min " +
        (if (cfg.minuteMillis != 60000L) s"(escala: 1 min = ${cfg.minuteMillis} ms)" else "") +
        s", destino=Kafka(${cfg.kafkaBootstrap}/${cfg.kafkaTopic})"
    )
  }
}
