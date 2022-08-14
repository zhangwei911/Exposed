plugins {
    kotlin("jvm") apply true
}

dependencies {
    api(project(":exposed-core"))
    api("org.springframework.security", "spring-security-crypto", "5.6.6")
}
