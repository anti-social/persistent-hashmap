import org.jetbrains.kotlin.gradle.dsl.Coroutines
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    java
    kotlin("jvm") version "1.2.41"
}

group = "company.evo"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    val kotlintestVersion = "3.1.6"

    compile(kotlin("stdlib-jdk8"))

    testCompile("io.kotlintest", "kotlintest-core", kotlintestVersion)
    testCompile("io.kotlintest", "kotlintest-assertions", kotlintestVersion)
    testCompile("io.kotlintest", "kotlintest-runner-junit5", kotlintestVersion)
    testCompile("commons-io", "commons-io", "2.6")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_8
}
tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

kotlin {
    experimental {
        coroutines = Coroutines.ENABLE
    }
}

val test by tasks.getting(Test::class) {
    useJUnitPlatform()
}
