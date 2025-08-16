package org.ktor_lecture.kafkalecture

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaLectureApplication

fun main(args: Array<String>) {
    runApplication<KafkaLectureApplication>(*args)
}
