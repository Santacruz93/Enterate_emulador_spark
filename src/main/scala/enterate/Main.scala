package enterate

object Main {
  private def argMap(args: Array[String]): Map[String,String] =
    args.sliding(2,2).collect { case Array(k, v) if k.startsWith("--") => k.drop(2).toUpperCase -> v }.toMap

  def main(args: Array[String]): Unit = {
    val m = argMap(args)

    val citizens   = m.getOrElse("CITIZENS", "200").toInt
    val zonesCsv   = m.getOrElse("ZONES_CSV", "zones.csv")
    val kafkaEn    = m.getOrElse("KAFKA_ENABLED", "true").toBoolean
    val kafkaBoot  = m.getOrElse("KAFKA_BOOTSTRAP", "localhost:9093")
    val kafkaTopic = m.getOrElse("KAFKA_TOPIC", "enterate_incidents")

    // Defaults: 2..15 minutos
    val gapMin     = m.getOrElse("GAP_MIN", "2").toInt
    val gapMax     = m.getOrElse("GAP_MAX", "15").toInt
    // 60000 = minuto real; 1000 => “1 min” sim = 1s real (pruebas)
    val minuteMs   = m.getOrElse("MINUTE_MS", "60000").toLong

    val endProb    = m.getOrElse("END_PROB", "0.25").toDouble
    val verbose    = m.getOrElse("VERBOSE", "true").toBoolean

    val cfg = Emulator.Config(
      citizens       = citizens,
      zonesCsv       = zonesCsv,
      kafkaEnabled   = kafkaEn,
      kafkaBootstrap = kafkaBoot,
      kafkaTopic     = kafkaTopic,
      gapMinMinutes  = gapMin,
      gapMaxMinutes  = gapMax,
      minuteMillis   = minuteMs,
      endProbability = endProb,
      verbose        = verbose
    )

    Emulator.start(cfg)

    // mantener proceso vivo
    while (true) Thread.sleep(60000)
  }
}
