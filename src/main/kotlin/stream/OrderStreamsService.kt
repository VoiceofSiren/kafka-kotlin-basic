package org.ktor_lecture.stream

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyWindowStore
import org.ktor_lecture.model.OrderCountComparisonStats
import org.ktor_lecture.model.PeriodStats
import org.ktor_lecture.model.WindowedOrderCount
import org.slf4j.LoggerFactory
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.stereotype.Service
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

@Service
class OrderStreamsService(
    // StreamsBuilderFactoryBean
    //  - Kafka Stream 인스턴스를 관리하는 Bean
    //  - Kafka Stream의 Window 저장소를 활용하여 시간 기반의 데이터를 조회하는 데 사용됨.
    private val factory: StreamsBuilderFactoryBean
) {

    private val logger = LoggerFactory.getLogger(OrderStreamsService::class.java)

    fun compareOrderCount(): OrderCountComparisonStats? {
        return try {
            val streams = factory.kafkaStreams
            if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
                return null
            }

            val store: ReadOnlyWindowStore<String, WindowedOrderCount> = streams.store(
                StoreQueryParameters
                    .fromNameAndType("order-count-store", QueryableStoreTypes.windowStore())
            )

            val now = Instant.now()

            val currentPeriodEnd = now
            val currentPeriodStart = now.minusSeconds(300) // 5분 전

            val previousPeriodEnd = currentPeriodStart
            val previousPeriodStart = currentPeriodStart.minusSeconds(300)

            val currentCount = countForPeriod(store, currentPeriodStart, currentPeriodEnd)
            val previousCount = countForPeriod(store, previousPeriodStart, currentPeriodEnd)

            val changeCount = currentCount - previousCount
            val changePercentage = if (previousCount > 0) {
                (changeCount.toDouble() / previousCount.toDouble()) * 100.0
            } else if (currentCount > 0) {
                100.0
            } else {
                0.0
            }

            OrderCountComparisonStats(
                currentPeriod = PeriodStats(
                    windowStart = LocalDateTime.ofInstant(currentPeriodStart, ZoneOffset.UTC),
                    windowEnd = LocalDateTime.ofInstant(currentPeriodEnd, ZoneOffset.UTC),
                    orderCount = currentCount
                ),
                previousPeriod = PeriodStats(
                    windowStart = LocalDateTime.ofInstant(previousPeriodStart, ZoneOffset.UTC),
                    windowEnd = LocalDateTime.ofInstant(previousPeriodEnd, ZoneOffset.UTC),
                    orderCount = previousCount
                ),
                changeCount = changeCount,
                changePercentage = changePercentage,
                isIncreasing = changeCount > 0
            )
        } catch (exception: Exception) {
            logger.error("Failed to get streams info", exception.message)
            return null
        }
    }

    private fun countForPeriod(
        store: ReadOnlyWindowStore<String, WindowedOrderCount>,
        startTime: Instant,
        endTime: Instant
    ): Long {
        var totalCount = 0L

        store.fetchAll(startTime, endTime)
            .use { iterator ->
                val entry = iterator.next()
                totalCount += entry.value.count
            }

        return totalCount
    }
}