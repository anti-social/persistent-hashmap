buildscript {
    repositories {
        mavenCentral()
    }
}
repositories {
    mavenCentral()
}

plugins {
    java
    kotlin("jvm")
    id("me.champeau.gradle.jmh") version "0.5.2"
}

dependencies {
    // jmh("org.openjdk.jmh", "jmh-core", "1.20")
    jmh(project(":"))
    jmh(kotlin("stdlib-jdk8"))
    jmh("net.sf.trove4j:core:3.1.0")
}

jmh {
    System.getProperty("jmh.include")?.let {
        include = it.split(',')
    }

    jvmArgs = listOf("-Dproject.dir=${rootDir}")

    warmupIterations = 1
    fork = 1
    iterations = 4
    timeOnIteration = "1s"
}
