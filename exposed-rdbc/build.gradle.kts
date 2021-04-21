import org.jetbrains.exposed.gradle.Versions

plugins {
    kotlin("jvm") apply true
}

repositories {
    mavenCentral()
}

dependencies {
    api(project(":exposed-jdbc"))
    api("io.r2dbc", "r2dbc-spi", "0.8.2.RELEASE")
    api("io.r2dbc", "r2dbc-postgresql", "0.8.2.RELEASE")
    api("io.r2dbc", "r2dbc-h2", "0.8.2.RELEASE") {
//        exclude("com.h2database")
    }
//    api("org.jetbrains.kotlinx", "kotlinx-coroutines-core", Versions.kotlinCoroutines)
    api("org.jetbrains.kotlinx", "kotlinx-coroutines-reactive", Versions.kotlinCoroutines)
}