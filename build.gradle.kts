plugins {
    java
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "su.primecorp"
version = "1.0.0"

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(21)) }
    // sourcesJar можно оставить, но он не нужен на сервере
    withSourcesJar()
}

repositories {
    mavenCentral()
    maven("https://repo.papermc.io/repository/maven-public/")
}

dependencies {
    compileOnly("io.papermc.paper:paper-api:1.21.8-R0.1-SNAPSHOT") // лучше совпадать с сервером
    implementation("com.zaxxer:HikariCP:5.1.0")
    implementation("org.slf4j:slf4j-api:2.0.16")
    implementation("mysql:mysql-connector-java:8.0.33")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.release.set(21)
}

tasks.shadowJar {
    archiveClassifier.set("") // делает shadow jar основным: PrimeRewardsApprover-1.0.0.jar
    // чтобы не тянуть лишнее, но можно убрать если не хочешь рисковать:
}

tasks.jar {
    enabled = false // чтобы не было "обычного" jar без зависимостей
}

tasks.build {
    dependsOn(tasks.shadowJar)
}
