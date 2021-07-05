import com.github.erizo.gradle.JcstressPluginExtension
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

buildscript {
    repositories {
        mavenCentral()
    }
    dependencies {
        // Comment kapt plugin before running jcstress tests
        classpath("com.github.erizo.gradle:jcstress-gradle-plugin:0.8.6")
    }
}

plugins {
    java
    `maven-publish`
    kotlin("jvm") version "1.5.20"
    kotlin("kapt") version "1.5.20"
    id("org.ajoberstar.grgit") version "3.1.1"
}

apply {
    plugin("jcstress")
}

val grgit: org.ajoberstar.grgit.Grgit by extra
val tag = grgit.describe(mapOf("tags" to true, "match" to listOf("v*"))) ?: "v0.0.0"

group = "company.evo"
version = tag.trimStart('v')

repositories {
    mavenCentral()
}

dependencies {
    val kotestVersion = "4.6.0"
    val lincheckVersion = "2.14.1"

    implementation(kotlin("stdlib-jdk8"))
    implementation(kotlin("reflect"))

    compileOnly(project(":processor"))
    kapt(project(":processor"))

    testImplementation("io.kotest", "kotest-runner-junit5", kotestVersion)
    testImplementation("io.kotest", "kotest-property", kotestVersion)
    testImplementation("org.jetbrains.kotlinx", "lincheck", lincheckVersion)
    testImplementation("commons-io", "commons-io", "2.6")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
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

kapt {
    arguments {
        arg("kotlin.source", kotlin.sourceSets["main"].kotlin.srcDirs.first())
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJar") {
            groupId = "company.evo"
            artifactId = "persistent-hashmap"
            version = project.version.toString()

            from(components["java"])
        }
    }
}
