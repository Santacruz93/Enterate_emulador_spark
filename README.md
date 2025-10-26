# ENTÉRATE · Emulador + Streaming (Spark)

Proyecto de **Ingeniería de Datos** que integra:

- **Emulador** (Scala) que simula ciudadanos reportando incidencias y las **publica en Kafka**.
- **Spark Structured Streaming** (local) que **consume Kafka**, aplica **reglas de negocio**, **escribe CSV** y **envía eventos a la API**.


## Tabla de contenidos

1. [Arquitectura](#arquitectura)
2. [Estructura del repo](#estructura-del-repo)
3. [Requisitos](#requisitos)
4. [Variables de entorno](#variables-de-entorno)
5. [Flags para la configuracion de la VM en Intellij con JDK 17](#Flags para la configuracion de la VM en Intellij con JDK 17)

---

## Arquitectura

<details> <summary>Ver diagrama ASCII (fallback)</summary>

[Emulator (Scala)] -- JSON --> [Kafka topic: enterate_incidents]
                                   |
                                   v
                        [Spark Structured Streaming]
                            |  Ventanas 5 min + Reglas
                            |--> CSV: out/incidents_decisions/
                            |--> POST JSON --> [FastAPI] --> [PostgreSQL/PostGIS]

</details>

- El **Emulador** genera eventos continuos (incidentes **new** / cierres **end**) de forma **aleatoria y constante** (intervalos entre **2 y 15 min** por “ciudadano virtual”).
- **Spark** lee de Kafka, agrega en **ventanas de 10 minutos** por **zona (distrito/barrio) y tipo**, decide y:
    - **Anexa** al CSV (para analítica y seguridad)
    - **POST** a la **API** (persistencia/consulta)

---

## Estructura del repo

├─ build.sbt

├─ project/ # (archivos SBT)

├─ docker-compose.yml # Kafka/ZooKeeper

├─ data/

    └─ zonas.csv # (distrito,barrio,lat_min,lat_max,lon_min,lon_max)
├─ out/
    
    └─ incidents_decisions/ # (CSV generados por Spark)
├─ checkpoints/

    └─ incidents_decisions/ # (estado streaming)
└─ src/
    
    └─ main/
        └─ scala/
            └─ enterate/
                ├─ Emulator.scala # publica a Kafka (JSON)
                └─ StreamingJob.scala # consume Kafka, reglas + CSV + POST API


---

## Requisitos

- **Java** 17 (JDK)
- **Scala** 2.12.x / **SBT** ≥ 1.9
- **Docker** ≥ 24 y **Docker Compose** ≥ 2
- **Git** ≥ 2.x
- (Opcional) **IntelliJ IDEA** con plugin de Scala

> **Windows**: si tu hostname tiene `_` (p. ej. `Compadora_Carlos`), Spark puede fallar. Este repo ya fuerza `SPARK_LOCAL_HOSTNAME=127.0.0.1` en el `StreamingJob`.

---



## Variables de entorno

# Kafka (listener externo mapeado por Docker)
KAFKA_BOOTSTRAP_SERVERS=localhost:9093
KAFKA_TOPIC=enterate_incidents

# Salidas Spark
OUTPUT_DIR=out/incidents_decisions
CHECKPOINT_DIR=checkpoints/incidents_decisions

# API destino desde Spark
ENTERATE_API_URL=http://localhost:8001/incidents/
CITY=Madrid

# Emulador: tiempos de envío (ms)
EMU_MIN_DELAY_MS=120000   # 2 min
EMU_MAX_DELAY_MS=900000   # 15 min

---

## Flags para la configuracion de la VM en Intellij con JDK 17

--add-exports=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED

---
