package org.ktor_lecture.avro

import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

@Component
class AvroOrderEventConsumer {

    private val logger = LoggerFactory.getLogger(AvroOrderEventConsumer::class.java)

    // Event Handler 등록
    // Event 소비
    @KafkaListener(
        topics = ["orders-avro"],
        groupId = "avro-order-processor-v1",
        containerFactory = "avroKafkaListenerContainerFactory"
    )
    fun handleOrderEventV1(
        // GenericRecord
        //  - Avro Schema에 따라 데이터를 동적으로 표현하는 객체
        //  - 특정 클래스에 바인딩되지 않고 동적으로 Avro 데이터를 다룸
        @Payload avroRecord: GenericRecord,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        @Header(KafkaHeaders.OFFSET) offset: Long,
    ) {
        try {
            val orderData = extractOrderDataFromAvro(avroRecord)
            logger.info("Processing Avro order: orderId=${orderData.orderId}, partition=$partition, offset=$offset")
        } catch (exception: Exception) {
            logger.error(exception.message, exception)
        }

    }

    // Avro 데이터에서 원하는 값을 추출
    private fun extractOrderDataFromAvro(record: GenericRecord): OrderDataV1 {
        return OrderDataV1(
            orderId = record["orderId"].toString(),
            customerId = record["customerId"].toString(),
            quantity = record["quantity"] as Int,
            price = convertBytesToPrice(record["price"] as ByteBuffer),
            timestamp = convertTimestamp(record["createdAt"] as Long),
            status = record["status"].toString(),
            version = record["version"] as Long,
        )
    }

    /**
     *  - order-entity.avsc
     *    {
     *       "name": "price",
     *       "type": {
     *         "type": "bytes",
     *         "logicalType": "decimal",
     *         "precision": 10,
     *         "scale": 2
     *       },
     *       "doc": "주문 가격"
     *     }
     */
    // Avro Schema에서 Decimal로 정의된 필드는 ByteBuffer 형태로 전달됨.
    // 변환 과정: ByteBuffer -> ByteArray -> BigInteger -> BigDecimal
    private fun convertBytesToPrice(byteBuffer: ByteBuffer): BigDecimal {
        val bytes = ByteArray(byteBuffer.remaining())
        byteBuffer.get(bytes)
        val bigInt = BigInteger(bytes)
        // scale 적용: 소숫점 아래 2자리까지
        return BigDecimal(bigInt, 2)
    }

    /**
     *  - order-entity.avsc
     *     {
     *       "name": "createdAt",
     *       "type": {
     *         "type": "long",
     *         "logicalType": "timestamp-millis"
     *       },
     *       "doc": "생성 시간"
     *     }
     */
    // Avro: Unix epoch millisecond 사용
    private fun convertTimestamp(epochMillis: Long): LocalDateTime {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC)
    }
}

data class OrderDataV1 (
    val orderId: String,
    val customerId: String,
    val quantity: Int,
    val price: BigDecimal,
    val timestamp: LocalDateTime,
    val status: String,
    val version: Long
)