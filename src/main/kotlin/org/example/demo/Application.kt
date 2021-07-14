package org.example.demo

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

val objectMapper: ObjectMapper = jacksonObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .registerModule(KotlinModule())

class Application(config: Config) {
    private val log = LoggerFactory.getLogger(this::class.java)
    val kafkaConfig: KafkaConfig = KafkaConfig(config)
    val fileIoConfig: FileIoApiConfig = FileIoApiConfig(config)
    val s3Config: S3Config = S3Config(config)

    val kafkaStream: KafkaStreamApp = KafkaStreamApp(kafkaConfig, FileIOApi(fileIoConfig), s3Config, S3ClientFactory(s3Config))

    fun run() {
        log.debug("Starting kafka stream...")
        kafkaStream.start()
    }

    fun stop() {
        log.debug("Stopping kafka stream...")
        kafkaStream.close()
    }
}

fun main() {
    Application(ConfigFactory.load()).run()
}
