lazy val root = (project in file("."))
  .settings(
    name         := "CSCI3395-",
    organization := "edu.trinity",
    scalaVersion := "2.11.8",
    version      := "0.1.0-SNAPSHOT",
    libraryDependencies += "org.scalafx" % "scalafx_2.11" % "8.0.102-R11"

  
  )
