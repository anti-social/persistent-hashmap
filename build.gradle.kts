import com.github.erizo.gradle.JcstressPluginExtension
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

buildscript {
    repositories {
        mavenCentral()
    }
    dependencies {
        // Comment kapt plugin before running jcstress tests
        classpath("io.github.reyerizo.gradle:jcstress-gradle-plugin:0.8.15")
    }
}

plugins {
    java
    `maven-publish`
    id("io.github.gradle-nexus.publish-plugin")
    signing
    kotlin("jvm")
    kotlin("kapt")
    id("org.ajoberstar.grgit") version "4.1.1"
    application
}

apply {
    plugin("jcstress")
}

val tag = grgit.describe(mapOf("tags" to true, "match" to listOf("v*"))) ?: "v0.0.0"

group = "dev.evo.persistent"
version = tag.trimStart('v')

repositories {
    mavenCentral()
}

dependencies {
    val slf4jVersion = "2.0.7"
    val kotestVersion = "4.6.0"
    val lincheckVersion = "2.14.1"

    implementation(kotlin("stdlib-jdk8"))
    implementation(kotlin("reflect"))
    implementation("org.slf4j", "slf4j-api", slf4jVersion)

    compileOnly(project(":processor"))
    kapt(project(":processor"))

    testImplementation("io.kotest", "kotest-runner-junit5", kotestVersion)
    testImplementation("io.kotest", "kotest-property", kotestVersion)
    testImplementation("org.jetbrains.kotlinx", "lincheck", lincheckVersion)
    testImplementation("commons-io", "commons-io", "2.6")
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = JavaVersion.VERSION_11.toString()
}

application {
    mainClass.set("dev.evo.persistent.hashmap.MainKt")
}

val test by tasks.getting(Test::class) {
    properties["seed"]?.let {
        systemProperties["test.random.seed"] = it
    }
    useJUnitPlatform()
    outputs.upToDateWhen { false }

    testLogging {
        events = mutableSetOf<TestLogEvent>().apply {
            add(TestLogEvent.FAILED)
            if (project.hasProperty("showPassedTests")) {
                add(TestLogEvent.PASSED)
            }
        }
        exceptionFormat = TestExceptionFormat.FULL
    }
}

configure<JcstressPluginExtension> {
    jcstressDependency = "org.openjdk.jcstress:jcstress-core:0.4"
}

kapt {
    arguments {
        arg("kotlin.source", kotlin.sourceSets["main"].kotlin.srcDirs.first())
    }
}

signing {
    sign(publishing.publications)
}

extra["projectUrl"] = uri("https://github.com/anti-social/persistent-hashmap")
configureJvmPublishing("persistent-hashmap", "Persistent concurrent hashmap implementation")
nexusPublishing {
    repositories {
        configureSonatypeRepository(project)
    }
}
