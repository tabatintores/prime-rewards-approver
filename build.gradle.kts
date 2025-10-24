plugins {
    java
}

group = "su.primecorp"
version = "1.0.0"

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(21)) }
    withSourcesJar()
}

repositories {
    mavenCentral()
    maven("https://repo.papermc.io/repository/maven-public/")
}

dependencies {
    compileOnly("io.papermc.paper:paper-api:1.21.1-R0.1-SNAPSHOT") // API совместима с 1.21.8
    implementation("com.zaxxer:HikariCP:5.1.0")
    implementation("org.slf4j:slf4j-api:2.0.16")
    implementation("mysql:mysql-connector-java:8.0.33")
    // для шаблонизации без лишних зависимостей обойдёмся своим классом
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.release.set(21)
}
