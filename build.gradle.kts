import com.github.erizo.gradle.JcstressPluginExtension
import org.jetbrains.kotlin.gradle.dsl.Coroutines
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
    kotlin("jvm") version "1.2.41"
}

apply {
    plugin("jcstress")
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
    // project.properties.subMap(["foo", "bar"])
    properties["seed"]?.let {
        systemProperties["test.random.seed"] = it
    }
    useJUnitPlatform()
}

configure<JcstressPluginExtension> {
    jcstressDependency = "org.openjdk.jcstress:jcstress-core:0.4"
}
