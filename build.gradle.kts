import com.github.erizo.gradle.JcstressPluginExtension
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

buildscript {
    repositories {
        mavenCentral()
    }
    dependencies {
        classpath("com.github.erizo.gradle:jcstress-gradle-plugin:0.8.1")
    }
}

plugins {
    java
    kotlin("jvm") version "1.3.21"
}

apply {
    plugin("jcstress")
}

group = "company.evo"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
    maven("https://dl.bintray.com/devexperts/Maven/")
}

dependencies {
    val kotlintestVersion = "3.1.11"
    val lincheckVersion = "2.0"

    implementation(kotlin("stdlib-jdk8"))
    implementation(kotlin("reflect"))

    testImplementation("io.kotlintest", "kotlintest-core", kotlintestVersion)
    testImplementation("io.kotlintest", "kotlintest-assertions", kotlintestVersion)
    testImplementation("io.kotlintest", "kotlintest-runner-junit5", kotlintestVersion)
    testImplementation("com.devexperts.lincheck", "lincheck", lincheckVersion)
    testImplementation("commons-io", "commons-io", "2.6")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_8
}
tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = JavaVersion.VERSION_1_8.toString()
}

val test by tasks.getting(Test::class) {
    properties["seed"]?.let {
        systemProperties["test.random.seed"] = it
    }
    useJUnitPlatform()
    outputs.upToDateWhen { false }
}

configure<JcstressPluginExtension> {
    jcstressDependency = "org.openjdk.jcstress:jcstress-core:0.4"
}
