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
    api(project(":spring-transaction"))
    api("org.springframework.boot", "spring-boot-starter-data-jdbc", "2.1.6.RELEASE")
    api("org.springframework.boot", "spring-boot-autoconfigure", "2.1.6.RELEASE")
    compileOnly("org.springframework.boot", "spring-boot-configuration-processor", "2.1.6.RELEASE")

    testImplementation("org.springframework.boot", "spring-boot-starter-test", "2.1.6.RELEASE")
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
