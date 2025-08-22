package org.ktor_lecture.stream

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.state.WindowStore
import org.ktor_lecture.model.FraudAlert
import org.ktor_lecture.model.FraudSeverity
import org.ktor_lecture.model.OrderEvent
import org.ktor_lecture.model.WindowedOrderCount
import org.ktor_lecture.model.WindowedSalesData
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.kafka.support.serializer.JsonSerde
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.time.Duration

@Component
// Topology 구성
// 4가지의 Stream을 처리
class OrderStreamsProcessor(
    // 수신되는 토픽
    @Value("\${kafka.topics.orders}")
    private val ordersTopic: String,
    // 송신되는 토픽
    @Value("\${kafka.topics.high-value-orders}")
    private val highValueOrdersTopic: String,
    // 송신되는 토픽
    @Value("\${kafka.topics.fraud-alerts}")
    private val fraudAlertsTopic: String,
) {
    private val logger = LoggerFactory.getLogger(OrderStreamsProcessor::class.java)

    // JsonSerde: Json Serialization & Deserialization
    //  - Kafka Streams 단에서 데이터를 직렬화하고 역직렬화하는 데 사용되는 클래스
    //  - 각각의 이벤트에 대하여 Serde를 생성
    private val orderEventSerde = createJsonSerde<OrderEvent>()
    private val fraudAlertSerde = createJsonSerde<FraudAlert>()
    private val windowedOrderCountSerde = createJsonSerde<WindowedOrderCount>()
    private val windowedSalesDataSerde = createJsonSerde<WindowedSalesData>()


    // inline 함수
    //  - 일반적인 함수 호출을 없애고 호출되는 곳에 함수 바디가 직접 삽입되어 성능 최적화
    //  - reified generic 사용을 위한 필수 전제
    // reified
    //  - Kotlin의 Generic은 기본적으로 타입 소거가 일어남.
    //  - Run-time에 T가 String인지 Int인지 알 수 없음.
    //  - inline 함수 안에서만 Generic 타입 정보를 Run-time에 유지할 수 있게 함.
    private inline fun <reified T> createJsonSerde(): JsonSerde<T> {
        return JsonSerde<T>().apply {
            configure(
                mapOf(
                    // 신뢰할 수 있는 패키지 (Optional)
                    "spring.json.trusted.packages" to "org.kafka_lecture.model",
                    // 타입 정보 헤더 (false - 필요하지 않음.)
                    "spring.json.add.type.headers" to false,
                    // 기본 타입 Generic 설정 - 원하는 타입을 유동적으로 받아서 Serde를 구성할 수 있음.
                    "spring.json.value.default.type" to T::class.java.name,
                ),
                false)
        }
    }

    @Bean
    // @Value("\${kafka.topics.orders}") ordersTopic에 대한 스트림 처리 (KStream)
    //  - 이벤트 전송 시 자동으로 해당 이벤트에 대하여 스트림을 처리한다.
    // Topology 타입의 Bean
    //  - Spring 애플리케이션이 처음 시작할 때 자동으로 등록된다.
    //  - 별도의 호출 없이 자동으로 실행된다.
    fun orderProcessingTopology(builder: StreamsBuilder): Topology {
        val orderStream: KStream<String, OrderEvent> = builder
            .stream(
                ordersTopic,
                Consumed.with(
                    // OrderEventPublisher.publishOrderEvent(OrderEvent)에서
                    // Partition 선택의 기준값인 Key를
                    // String 타입의 orderEvent.orderId로 설정하였기 때문
                    Serdes.String(),
                    orderEventSerde
                )
            )

        highValueStream(orderStream)
        fraudStream(orderStream)
        orderCountStatsStream(orderStream)
        salesStatsStream(orderStream)

        return builder.build()
    }

    fun highValueStream(orderStream: KStream<String, OrderEvent>) {
        val highValueStream = orderStream.filter { _, orderEvent ->
            logger.info("Filtering high-value Stream Order: {}", orderEvent.orderId)
            orderEvent.price >= BigDecimal(1000)
        }

        // 토픽에 메시지 전송
        highValueStream.to(
            highValueOrdersTopic,
            Produced.with(
                Serdes.String(),
                orderEventSerde
            )
        )
    }

    private fun fraudStream(orderStream: KStream<String, OrderEvent>) {
        val fraudStream = orderStream.filter { _, orderEvent ->
            orderEvent.price >= BigDecimal(5000) ||
                    orderEvent.quantity >= 100 ||
                    orderEvent.price.multiply(BigDecimal.valueOf(orderEvent.quantity.toLong())) >= BigDecimal(10000)
        }.mapValues { orderEvent ->
            val reason = when {
                orderEvent.price >= BigDecimal(5000) -> "High price for a single order"
                orderEvent.quantity > 100 -> "High quantity for a single order"
                else -> "High total order value"
            }

            val severity = when {
                orderEvent.price >= BigDecimal(10000) -> FraudSeverity.CRITICAL
                orderEvent.price >= BigDecimal(5000) -> FraudSeverity.HIGH
                orderEvent.quantity > 100 -> FraudSeverity.MEDIUM
                else -> FraudSeverity.LOW
            }

            FraudAlert (
                orderId = orderEvent.orderId,
                customerId = orderEvent.customerId,
                reason = reason,
                severity = severity,
            )
        }

        fraudStream.to(
            fraudAlertsTopic,
            Produced.with(
                Serdes.String(),
                fraudAlertSerde
            )
        )
    }

    private fun orderCountStatsStream(orderStream: KStream<String, OrderEvent>) {
        orderStream
            .groupByKey(
                Grouped.with(
                    // orderStream의 Key가 OrderEvent.orderId이므로
                    // customerId로 그룹핑할 수 없음.
                    Serdes.String(),
                    orderEventSerde)
            )
            .windowedBy(
                // 10초 단위로 window 생성
                //  - 연속적인 데이터 스트림에 대해서 10초마다 구간을 나누어 집계
                TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10))
            )
            .aggregate(
                { WindowedOrderCount() },
                // 동일한 윈도우 (10초 구간) 내에서 동일한 Key가 수신될 경우 increment()를 호출해 count 값을 증가시킴.
                // aggregate: WindowedOrderCount 객체
                { xxorderIdxx, xxorderEventxx, aggregate -> aggregate.increment()},
                // "order-counts-store" 이름의 Kafka 로컬 저장소에 저장
                //  - Key 타입은 String
                //  - Value는 WindowedOrderCount 값
                Materialized.`as`<String, WindowedOrderCount, WindowStore<Bytes, ByteArray>>("order-counts-store")
                    .withValueSerde(windowedOrderCountSerde)
            )
    }

    private fun salesStatsStream(orderStream: KStream<String, OrderEvent>) {
        /*
          [입력 스트림]
          <"customer1", OrderEvent(orderId="order1", customerId="customer1", price=100)>
          <"customer2", OrderEvent(orderId="order2", customerId="customer2", price=200)>
          <"customer1", OrderEvent(orderId="order3", customerId="customer1", price=150)>

          [groupBy 사용 후]
          customer1: [OrderEvent(order1, 100), OrderEvent(order3, 150)]
          customer2: [OrderEvent(order2, 200)]
        */
        orderStream
            .groupBy(
                { key, orderEvent -> orderEvent.customerId},
                Grouped.with(
                    Serdes.String(),
                    orderEventSerde)
            )
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)))
            .aggregate(
                { WindowedSalesData()},
                { _, orderEvent, aggregate -> aggregate.add(orderEvent.price)},
                Materialized.`as`<String, WindowedSalesData, WindowStore<Bytes, ByteArray>>("sales-stats-store")
                    .withValueSerde(windowedSalesDataSerde)
            )
    }
}