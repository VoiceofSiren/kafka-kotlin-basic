package org.ktor_lecture.avro

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.Schema
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.core.io.ClassPathResource
import org.springframework.stereotype.Component
import java.io.IOException

@Component
class SchemaManager(
    @Value("\${schema.registry.url:http://localhost:8081")
    private val schemaRegistryUrl: String,
): ApplicationRunner {

    private val logger = LoggerFactory.getLogger(SchemaManager::class.java)

    private final val avroFileResource = "avro/order-entity.avsc"
    // Subject 변수의 Naming convention: topic - name - key - value
    private final val subject = "orders-avro-value"
    private var cachedSchema: Schema? = null
    private val client: SchemaRegistryClient = CachedSchemaRegistryClient(schemaRegistryUrl, 100)

    fun orderEventSchema(): Schema {
        return cachedSchema ?: loadSchemaFromRegistry().also {
            cachedSchema = it
        }
    }

    private fun loadSchemaFromRegistry(): Schema {
        return try {
            val latest = client.getLatestSchemaMetadata(subject)
            val schemaString = latest.schema

            Schema.Parser().parse(schemaString)
        } catch (exception: Exception) {
            logger.warn("Failed to load schema from register", exception)
            loadSchemaFromFile(avroFileResource)
        }
    }

    private fun loadSchemaFromFile(path: String): Schema {
        return try {
            logger.info("path: {}", path)
            val resource = ClassPathResource(path)
            val content = resource.inputStream
                .bufferedReader()
                .use { it.readText() }
            Schema.Parser().parse(content)
        } catch (exception: IOException) {
            throw IllegalStateException("Failed to load schema from file: $path", exception)
        }
    }

    private fun registerSchemaIfNotExists(): Int? {
        return try {
            val existing = try {
                client.getAllVersions(subject)
            } catch (exception: Exception) {
                logger.info("subject does not exist")
                emptyList<Int>()
            }

            if (existing.isNotEmpty()) {
                // 최신 Schema를 넘겨 준다.
                val latest = client.getLatestSchemaMetadata(subject)
                return latest.id
            }

            val resource = loadSchemaFromFile(avroFileResource)
            val avroSchema = AvroSchema(resource)
            val id = client.register(subject, avroSchema)

            return id
        } catch (exception: Exception) {
            logger.error("Failed to register schema", exception)
            null
        }
    }

    // ApplicationRunner 구현 시 반드시 override해야 하는 메서드
    // 애플리케이션 시작시에 자동으로 호출됨.
    override fun run(args: ApplicationArguments?) {
        registerSchemaIfNotExists()
    }

}