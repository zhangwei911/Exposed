import org.jetbrains.kotlin.gradle.dsl.KotlinJvmCompile

plugins {
    kotlin("jvm") version "1.5.30" apply true
    id("io.github.gradle-nexus.publish-plugin") apply true
}

allprojects {
    if (this.name != "exposed-tests" && this != rootProject) {
        apply(from = rootProject.file("buildScripts/gradle/publishing.gradle.kts"))
    }
}

tasks.withType(JavaCompile::class) {
    targetCompatibility = "1.8"
}

tasks.withType<KotlinJvmCompile> {
    kotlinOptions {
        jvmTarget = "1.8"
        languageVersion = "1.5"
        apiVersion = "1.5"
    }
}

nexusPublishing {
    repositories {
        sonatype {
            username.set(System.getenv("exposed.sonatype.user"))
            password.set(System.getenv("exposed.sonatype.password"))
            useStaging.set(true)
        }
    }
}

repositories {
    mavenCentral()
}
