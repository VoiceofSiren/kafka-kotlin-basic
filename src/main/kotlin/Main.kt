package org.ktor_lecture

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.EnableKafkaStreams

@SpringBootApplication
@EnableKafka
// StreamsBuilderFactoryBean 활성화
@EnableKafkaStreams
class KafkaLectureApplication

fun main(args: Array<String>) {
    runApplication<KafkaLectureApplication>(*args)
}
