plugins {
    id("idea")
    id("scala")
}

repositories {
    mavenCentral()
}

dependencies {
    listOf("spark-core", "spark-sql", "spark-hive").forEach { name ->
        implementation("org.apache.spark:${name}_2.12:3.5.1")
    }
    implementation("org.scala-lang:scala-library:2.12.19")

    testImplementation("org.scalatest:scalatest_2.12:3.2.17")
    testImplementation("org.scalatestplus:scalatestplus-junit_2.12:1.0.0-M2")
}

tasks.test {
    jvmArgs = listOf("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED", "--add-exports", "java.base/sun.security.action=ALL-UNNAMED")
}

tasks.register<Zip>("packageTest") {
    archiveFileName.set("audax-data-engineering-test.zip")
    destinationDirectory.set(layout.buildDirectory.dir("dist"))

    from(layout.projectDirectory).include("src/test/scala/question/MovieRatingsTest.scala",
        "src/test/resources/movies_details.csv", "src/test/resources/movies_review.csv",
        "build.gradle.kts", "gradle.properties", "settings.gradle.kts",
        "README.md", "gradlew", "gradle/**/*")
}