repositories {
    mavenCentral()
}

plugins {
    java
    kotlin("jvm")
    id("me.champeau.jmh") version "0.6.6"
}

dependencies {
    jmh(project(":"))
    jmh(kotlin("stdlib-jdk8"))
    jmh("net.sf.trove4j:core:3.1.0")
}

tasks.getByName("jmh") {
    outputs.upToDateWhen { false }
}

jmh {
    System.getProperty("jmh.include")?.let {
        includes.set(it.split(','))
    }

    System.getProperty("jmh.params")?.let {
        benchmarkParameters.set(
            it.split(':')
                .associate { e ->
                    e.substringBefore('=') to objects.listProperty<String>().value(e.substringAfter('=').split(","))
                }
        )
    }

    jvmArgs.set(listOf("-Dproject.dir=${rootDir}"))

    warmupIterations.set(2)
    fork.set(1)
    iterations.set(4)
    timeOnIteration.set("1s")
}
