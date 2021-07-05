repositories {
    mavenCentral()
}

plugins {
    java
    kotlin("jvm")
    id("me.champeau.jmh") version "0.6.5"
}

dependencies {
    // jmh("org.openjdk.jmh", "jmh-core", "1.20")
    jmh(project(":"))
    jmh(kotlin("stdlib-jdk8"))
    jmh("net.sf.trove4j:core:3.1.0")
}

jmh {
    System.getProperty("jmh.include")?.let {
        includes.set(it.split(','))
    }

    jvmArgs.set(listOf("-Dproject.dir=${rootDir}"))

    warmupIterations.set(1)
    fork.set(1)
    iterations.set(4)
    timeOnIteration.set("1s")
}
