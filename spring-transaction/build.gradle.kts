import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    kotlin("jvm") apply true
}

repositories {
    mavenCentral()
}

dependencies {
    api(project(":exposed"))
    api("org.springframework", "spring-jdbc", "5.1.7.RELEASE")
    api("org.springframework", "spring-context", "5.1.7.RELEASE")
    implementation("org.jetbrains.kotlinx", "kotlinx-coroutines-core", "1.3.0-M1")
    implementation("com.h2database", "h2", "1.4.199")

    testImplementation(kotlin("test-junit"))
    testImplementation("org.springframework", "spring-test", "5.1.7.RELEASE")
    testImplementation("org.slf4j", "slf4j-log4j12", "1.7.26")
    testImplementation("log4j", "log4j", "1.2.17")
    testImplementation("junit", "junit", "4.12")
    testImplementation("org.hamcrest", "hamcrest-library", "1.3")
    testImplementation("com.h2database", "h2", "1.4.199")
}

tasks.withType(Test::class.java) {
    jvmArgs = listOf("-XX:MaxPermSize=256m")
    testLogging {
        events.addAll(listOf(TestLogEvent.PASSED, TestLogEvent.FAILED, TestLogEvent.SKIPPED))
        showStandardStreams = true
        exceptionFormat = TestExceptionFormat.FULL
    }
}
