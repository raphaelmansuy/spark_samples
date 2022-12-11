name := "jdbcspark"

version := "1.0"


val sparkVersion = "3.3.0"
val scalaLanguageVersion = "2.12"

scalaVersion := scalaLanguageVersion + ".17"


libraryDependencies += "org.scala-lang" % "scala-library" % scalaVersion.value

// Add spark dependencies
libraryDependencies += "org.apache.spark" % f"spark-core_$scalaLanguageVersion" % sparkVersion
libraryDependencies += "org.apache.spark" % f"spark-sql_$scalaLanguageVersion" % sparkVersion

// Import delta.io dependencies
libraryDependencies += "io.delta" % f"delta-core_$scalaLanguageVersion" % "2.1.1"

// JDBC dependencies
libraryDependencies += "org.postgresql" % "postgresql" % "42.5.1"

// Add AWS SDK dependencies
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.12.109"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.14" % "test"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.14"

// Add test containers dependencies
// import test containers dependencies

libraryDependencies += "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.40.11" % "test"

resolvers += "Artima Maven Repository" at "https://repo.artima.com/releases"








