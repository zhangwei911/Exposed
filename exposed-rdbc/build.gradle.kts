import tanvd.kosogor.proxy.publishJar
import org.jetbrains.exposed.gradle.Versions

plugins {
    kotlin("jvm") apply true
}

repositories {
    jcenter()
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

publishJar {
    publication {
        artifactId = "exposed-rdbc"
    }

    bintray {
        username = project.properties["bintrayUser"]?.toString() ?: System.getenv("BINTRAY_USER")
        secretKey = project.properties["bintrayApiKey"]?.toString() ?: System.getenv("BINTRAY_API_KEY")
        repository = "exposed"
        info {
            publish = false
            githubRepo = "https://github.com/JetBrains/Exposed.git"
            vcsUrl = "https://github.com/JetBrains/Exposed.git"
            userOrg = "kotlin"
            license = "Apache-2.0"
        }
    }
}