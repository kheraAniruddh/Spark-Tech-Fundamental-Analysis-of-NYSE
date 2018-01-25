
name := "BDAD_Forecasting"

version := "0.1"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"

// https://mvnrepository.com/artifact/com.databricks/spark-csv
libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.0"

// https://mvnrepository.com/artifact/com.cloudera.sparkts/sparkts
libraryDependencies += "com.cloudera.sparkts" % "sparkts" % "0.4.1"
        