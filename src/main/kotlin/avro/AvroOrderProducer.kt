package org.ktor_lecture.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.LocalDateTime
import java.time.ZoneOffset

@Service
class AvroOrderEventProducer (
    // avroKafkaTemplate: config.KafkaConfig에 Bean으로 정의됨.
    private val avroKafkaTemplate: KafkaTemplate<String, GenericRecord>,
    private val schemaManager: SchemaManager,
) {

    private val logger = LoggerFactory.getLogger(AvroOrderEventProducer::class.java)

    fun publishOrderEvent(
        orderId: String,
        customerId: String,
        quantity: Int,
        price: BigDecimal,
    ) {
        try {
            val schema = schemaManager.orderEventSchema()
            val avroRecord = createAvroRecord(
                orderId = orderId,
                customerId = customerId,
                quantity = quantity,
                price = price,
                schema = schema,
            )

            avroKafkaTemplate.send("orders-avro", orderId, avroRecord)
                .whenComplete { result, exception ->
                    if (exception == null) {
                        logger.info("Successfully published Avro record for order: orderId={}, partition={}, offset={}",
                            orderId, result?.recordMetadata?.partition(), result?.recordMetadata?.offset())
                    } else {
                        logger.error("Failed to publish Avro order event with orderId {}", orderId, exception)
                    }
                }

        } catch (exception: Exception) {
            logger.error("Error creating Avro record for order: orderId {}", orderId, exception)
        }
    }

    private fun createAvroRecord(
        orderId: String,
        customerId: String,
        quantity: Int,
        price: BigDecimal,
        schema: Schema
    ): GenericRecord {
        val record = GenericData.Record(schema)

        val now = LocalDateTime.now()
            .toInstant(ZoneOffset.UTC)
            .toEpochMilli()

        record.put("orderId", orderId)
        record.put("customerId", customerId)
        record.put("quantity", quantity)
        record.put("price", convertPriceToBytes(price))
        record.put("status", GenericData.EnumSymbol(
            schema.getField("status").schema(),
            "PENDING"))
        record.put("createdAt", now)
        record.put("updatedAt", now)
        record.put("version", 1L)

        return record
    }

    private fun convertPriceToBytes(price: BigDecimal): ByteBuffer {
        val scaled = price.setScale(2)
        val unscaledValue = scaled.unscaledValue()
        return ByteBuffer.wrap(unscaledValue.toByteArray())
    }

}