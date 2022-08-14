plugins {
    kotlin("jvm") apply true
    id("testWithDBs")
}

dependencies {
    api(project(":exposed-core"))
    api("joda-time", "joda-time", "2.10.13")
    testImplementation(project(":exposed-dao"))
    testImplementation(project(":exposed-tests"))
    testImplementation("junit", "junit", "4.12")
    testImplementation(kotlin("test-junit"))
}
