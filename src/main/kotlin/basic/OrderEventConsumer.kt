package org.ktor_lecture.basic

import org.ktor_lecture.model.OrderEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class OrderEventConsumer {
    private val logger = LoggerFactory.getLogger(OrderEventConsumer::class.java)

    /**
     *  Kafka Message Consumer를 등록
     *      - Spring boot에서 annotation scan 작업을 수행할 때
     *        @KafkaListener 애너테이션이 붙은 메서드들에 대하여 스캔하고
     *        자동으로 이 메서드들에 대하여 Kafka Message Listener Container를 생성한다.
     *      - Consumer가 Kafka Cluster와 연결을 진행한다면
     *        @KafkaListener의 topics 속성에서 지정한 토픽을 기준으로
     *        메시지를 주기적으로 폴링하면서 수신한다.
     */
    @KafkaListener(
        topics = ["\${kafka.topics.orders}"],
        groupId = "order-processing-group",
        concurrency = "3",  // 병렬 처리를 위한 스레드의 개수를 지정
                            // 일반적으로 topic에 있는 partition의 개수만큼 지정하는 것이 최적화된 설정임.
        containerFactory = "orderEventKafkaListenerContainerFactory", // config.KafkaConfig에 @Bean으로 정의되어 있음.
    )
    fun processOrder(
        @Payload orderEvent: OrderEvent,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: String,
        @Header(KafkaHeaders.OFFSET) offset: Long,
        ) {
        logger.info("Received OrderEvent: {}", orderEvent)

        try {
            processLogic()

            logger.info("Received OrderEvent: {}", orderEvent)
        } catch (exception: Exception) {
            logger.error("Error occurred while processing OrderEvent", exception)
        }
    }

    private fun processLogic() {
        Thread.sleep(100L)
    }
}

@Component
class OrderAnalyticsConsumer {
    private val logger = LoggerFactory.getLogger(OrderAnalyticsConsumer::class.java)

    @KafkaListener(
        topics = ["\${kafka.topics.orders}"],
        groupId = "order-analytics-group",
        concurrency = "2",
        containerFactory = "orderEventKafkaListenerContainerFactory",
    )
    fun collectAnalytics(
        @Payload orderEvent: OrderEvent,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int
    ) {
        logger.info("Collecting analytics for OrderEvent {} in partition {}", orderEvent.orderId, partition)

        try {
            updateCustomerStatistics(orderEvent)

        } catch (exception: Exception) {
            logger.error("Error occurred while collecting analytics for order ${orderEvent.orderId}: {}", exception.message)
        }
    }

    private fun updateCustomerStatistics(orderEvent: OrderEvent) {
        logger.debug("Updated customer statistics for customer {}", orderEvent.customerId)
    }

}

@Component
class OrderNotificationConsumer {
    private val logger = LoggerFactory.getLogger(OrderNotificationConsumer::class.java)

    @KafkaListener(
        topics = ["\${kafka.topics.orders}"],
        groupId = "order-notification-group",
        concurrency = "1",
        containerFactory = "orderEventKafkaListenerContainerFactory",
    )
    fun sendNotifications(
        @Payload orderEvent: OrderEvent,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int
    ) {
        logger.info("Sending notifications for order {} from partition {}", orderEvent.orderId, partition)

        try {
            if (isHighValueOrder(orderEvent)) {
                sendHighValueOrderStatus(orderEvent)
            }
        } catch (exception: Exception) {
            logger.error("Error occurred while sending notifications for order ${orderEvent.orderId}: {}", exception.message)
        }
    }

    private fun sendHighValueOrderStatus(orderEvent: OrderEvent) {
        logger.info("SMS sent for high-value order {}", orderEvent.orderId)
    }

    private fun isHighValueOrder(orderEvent: OrderEvent): Boolean {
        return orderEvent.price >= BigDecimal(1000)
    }
}