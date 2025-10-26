ThisBuild / scalaVersion := "2.12.20"
ThisBuild / organization := "com.enterate"
ThisBuild / version      := "0.2.0"

lazy val root = (project in file("."))
  .settings(
    name := "enterate-emulador-spark",

    libraryDependencies ++= Seq(
      // Spark local desde sbt (sin "provided" para poder ejecutar con runMain)
      "org.apache.spark" %% "spark-sql"            % "3.5.1",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",

      // Emulador -> Kafka
      "org.apache.kafka" %  "kafka-clients"        % "3.7.0",

      // Logs simples al correr con sbt (no estorba a Spark 3.5)
      "org.slf4j" % "slf4j-simple" % "1.7.36" % "runtime"
    ),

    Compile / mainClass := None,

    // ensamblado opcional por si quieres spark-submit
    assembly / assemblyJarName := "enterate-assembly-0.2.0.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _@_*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },

    // Ejecutar Spark embebido con JDK17 (abre módulos si lo necesitas)
    Compile / run / fork := true,
    Compile / run / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED"
    )
  )

import sbtassembly.AssemblyPlugin.autoImport._