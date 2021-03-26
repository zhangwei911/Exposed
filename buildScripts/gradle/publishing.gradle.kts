import org.jetbrains.exposed.gradle.*

apply(plugin = "java-library")
apply(plugin = "maven")
apply(plugin = "maven-publish")
apply(plugin = "signing")

val sourcesJar by tasks.registering(Jar::class) {
    archiveClassifier by "sources"
    from(project.file("src/main/kotlin"))
}

_publishing {
    publications {
        create<MavenPublication>("ExposedJars") {
            artifactId = project.name
            from(project.components["java"])
            artifact(sourcesJar.get())
            pom {
                configureMavenCentralMetadata(project)
            }
            signPublicationIfKeyPresent(project)
        }
    }
}
