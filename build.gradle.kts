plugins {
    id("org.springframework.boot") version "3.5.4"
    id("io.spring.dependency-management") version "1.1.7"
    kotlin("jvm") version "1.9.25"
    kotlin("plugin.spring") version "1.9.25"
}

group = "org.ktor_lecture"
version = "0.0.1-SNAPSHOT"
description = "kafka-lecture"

java {
    sourceCompatibility = JavaVersion.VERSION_17
}

configurations {
    compileOnly {
        extendsFrom(configurations.annotationProcessor.get())
    }
}

repositories {
    mavenCentral()
    maven {
        url = uri("http://packages.confluent.io/maven/")
        isAllowInsecureProtocol = true
    }
}

dependencies {

    // 웹 애플리케이션 개발을 위한 스타터 패키지
    // @Service 등 지원
    implementation("org.springframework.boot:spring-boot-starter-web")

    // JPA
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")

    // Kafka를 활용하기 위한 라이브러리
    implementation("org.springframework.kafka:spring-kafka")
    // Kafka의 메시지 및 스트림 처리
    implementation("org.apache.kafka:kafka-streams")

    // 데이터 직렬화를 위한 Avro 라이브러리
    implementation("org.apache.avro:avro:1.11.3")
    // Avro 데이터 다루기 위한 역/직렬화
    implementation("io.confluent:kafka-avro-serializer:7.5.0")
    // Avro 타입을 저장하는 SerDE를 지원하기 위한 라이브러리
    implementation("io.confluent:kafka-streams-avro-serde:7.5.0")
    // Avro 스키마를 저장하는 저장소
    implementation("io.confluent:kafka-schema-registry-client:7.5.0")

    // Jackson: 기본적인 역/직렬화
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")

    // spring, java 언어를 kotlin으로 맵핑해주는 라이브러리
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    // 애플리케이션 모니터링 라이브러리
    implementation("org.springframework.boot:spring-boot-starter-actuator")

    // runtime에만 동작하는 h2 및 postgreSQL CDC
    runtimeOnly("com.h2database:h2")
    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}
