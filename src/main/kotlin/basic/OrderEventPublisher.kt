package org.ktor_lecture.basic

import org.ktor_lecture.model.OrderEvent
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class OrderEventPublisher (
    private val kafkaTemplate: KafkaTemplate<String, OrderEvent>,
    @Value("\${kafka.topics.orders}")
    private val orderTopic: String
) {
    private val logger = LoggerFactory.getLogger(OrderEventPublisher::class.java)

    fun publishOrderEvent(orderEvent: OrderEvent) {
        try {
            // 1) Topic: Event 송수신의 기준
            // 2) Key: Partition 선택의 기준값 (Optional)
            // 3) Value: Event 값
            kafkaTemplate.send(orderTopic, orderEvent.orderId, orderEvent)
                .whenComplete { _, exception ->
                    if (exception != null) {
                        logger.error("Error occurred while publishing OrderEvent", exception)
                    } else {
                        logger.info("Successfully published OrderEvent")
                    }
                }
        } catch (exception: Exception) {
            logger.error("Error occurred while publishing OrderEvent", exception)
        }
    }
}