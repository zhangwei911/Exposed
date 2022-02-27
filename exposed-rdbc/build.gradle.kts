import org.jetbrains.exposed.gradle.Versions

plugins {
    kotlin("jvm") apply true
}

repositories {
    mavenCentral()
}

dependencies {
    api(project(":exposed-jdbc"))
    api("io.r2dbc", "r2dbc-spi", Versions.r2dbcSPI)

    api("org.jetbrains.kotlinx", "kotlinx-coroutines-reactive", Versions.kotlinCoroutines)
}
