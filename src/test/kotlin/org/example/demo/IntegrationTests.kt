package org.example.demo

import com.fasterxml.jackson.module.kotlin.readValue
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.Serializer
import org.awaitility.Awaitility
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.stream.IntStream
import kotlin.concurrent.thread
import kotlin.test.junit5.JUnit5Asserter

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IntegrationTests: TestContainersBase() {

    private lateinit var app: Application

    private val config = ConfigFactory.load()
    private val testProducer = UploadFileEventsTestProducer(config)
    private val testSuccessEventsConsumer = UploadFileSuccessEventsTestConsumer(config).also { it.poll() }
    private val testFailureEventsConsumer = UploadFileFailureEventsTestConsumer(config).also { it.poll() }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    inner class HappyPath {

        init {
            app = Application(config).also { it.run() }

            with(app) {
                // not the best healthcheck for kafka, but at least app context loads
                Awaitility.await().untilAsserted {
                    JUnit5Asserter.assertTrue("Kafka stream app is not running!", kafkaStream.isRunning())
                }
            }
        }

        @AfterAll
        fun closeStream() {
            app.stop()
        }

        @Test
        fun `should upload file to s3, and publish success event`() {
            val uploadFileId = UUID.randomUUID()
            testProducer.send(UploadFileEvent(uploadFileId, "a-valid-key"))
            Awaitility.await().untilAsserted {
                JUnit5Asserter.assertTrue("No success upload file event found of that id: $uploadFileId",
                    testSuccessEventsConsumer.messages.any {
                        it.value().id == uploadFileId && it.value().s3Tag.isNotEmpty()
                    }
                )
            }
        }

        @Test
        fun `should handle multiple events`() {
            IntStream.rangeClosed(1, 20).forEach {
                testProducer.send(UploadFileEvent(UUID.randomUUID(), "a-valid-key"))
            }
            Awaitility.await().untilAsserted {
                JUnit5Asserter.assertTrue("Size of success events is not right!",
                testSuccessEventsConsumer.messages.size >= 20)
            }
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    inner class UnhappyPath {

        init {
            System.setProperty("S3_BUCKET", "invalid-bucket-name")
            ConfigFactory.invalidateCaches() // used only for tests
            app = Application(ConfigFactory.load()).also { it.run() }

            with(app) {
                // not the best healthcheck for kafka, but at least app context loads
                Awaitility.await().untilAsserted {
                    JUnit5Asserter.assertTrue("Kafka stream app is not running!", kafkaStream.isRunning())
                }
            }
        }

        @AfterAll
        fun closeStream() {
            app.stop()
        }

        @Test
        fun `should not upload file to s3 when file is not found, and publish failure event`() {
            val uploadFileId = UUID.randomUUID()
            testProducer.send(UploadFileEvent(uploadFileId, "an-invalid-key"))
            Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted {
                JUnit5Asserter.assertTrue("No failure upload file event found of that id: $uploadFileId",
                    testFailureEventsConsumer.messages.any {
                        it.value().id == uploadFileId && it.value().reason.isNotEmpty()
                    }
                )
            }
        }

        @Test
        fun `should not upload file to s3 when bucket is not found, and publish failure event`() {
            val uploadFileId = UUID.randomUUID()
            testProducer.send(UploadFileEvent(uploadFileId, "a-valid-key"))
            Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted {
                JUnit5Asserter.assertTrue("No failure upload file event found of that id: $uploadFileId",
                    testFailureEventsConsumer.messages.any {
                        it.value().id == uploadFileId && it.value().reason.isNotEmpty()
                    }
                )
            }
        }
    }
}

// internals //
private class UploadFileEventsTestProducer(appConfig: Config) {
    private val log = LoggerFactory.getLogger(this::class.java)

    private val kProducer: KafkaProducer<Long, UploadFileEvent>
    private val topic: String = UploadFileEvent.topic

    init {
        val bootstrapServers = appConfig.getString("kafka.bootstrapServers")
        val config = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to LongSerializer::class.java.canonicalName,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to UploadFileEventSerializer::class.java.canonicalName
        )
        kProducer = KafkaProducer<Long, UploadFileEvent>(config)
    }

    fun send(message: UploadFileEvent) {
        log.debug("Sending test event: $message")
        kProducer.send(ProducerRecord(topic, message))
    }
}

private class UploadFileSuccessEventsTestConsumer(appConfig: Config) {
    private val log = LoggerFactory.getLogger(this::class.java)

    private val kConsumer: KafkaConsumer<Long, UploadFileSuccessEvent>
    private val topic: String = UploadFileSuccessEvent.topic
    val messages: BlockingQueue<ConsumerRecord<Long, UploadFileSuccessEvent>>

    init {
        val bootstrapServers = appConfig.getString("kafka.bootstrapServers")
        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to LongDeserializer::class.java.canonicalName,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to UploadFileSuccessEventDeserializer::class.java.canonicalName,
            ConsumerConfig.GROUP_ID_CONFIG to "test-upload-file-success-events-group-v1",
        )
        kConsumer = KafkaConsumer<Long, UploadFileSuccessEvent>(config)
        kConsumer.subscribe(listOf(topic))
        messages = LinkedBlockingQueue()
    }

    fun poll() = thread {
        while (true) {
            kConsumer.poll(Duration.ofMillis(100)).forEach { record ->
                log.debug("Consumed test event: ${record.value()}")
                messages.add(record)
            }
        }
    }
}

private class UploadFileFailureEventsTestConsumer(appConfig: Config) {
    private val log = LoggerFactory.getLogger(this::class.java)

    private val kConsumer: KafkaConsumer<Long, UploadFileFailureEvent>
    private val topic: String = UploadFileFailureEvent.topic
    val messages: BlockingQueue<ConsumerRecord<Long, UploadFileFailureEvent>>

    init {
        val bootstrapServers = appConfig.getString("kafka.bootstrapServers")
        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to LongDeserializer::class.java.canonicalName,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to UploadFileFailureEventDeserializer::class.java.canonicalName,
            ConsumerConfig.GROUP_ID_CONFIG to "test-upload-file-failure-events-group-v1",
        )
        kConsumer = KafkaConsumer<Long, UploadFileFailureEvent>(config)
        kConsumer.subscribe(listOf(topic))
        messages = LinkedBlockingQueue()
    }

    fun poll() = thread {
        while (true) {
            kConsumer.poll(Duration.ofMillis(100)).forEach { record ->
                log.info("Consumed test event: ${record.value()}")
                messages.add(record)
            }
        }
    }
}

// serdes //
class UploadFileEventSerializer: Serializer<UploadFileEvent> {
    override fun serialize(topic: String?, data: UploadFileEvent?): ByteArray? =
        data?.let { objectMapper.writeValueAsBytes(data) }
}
class UploadFileSuccessEventDeserializer: Deserializer<UploadFileSuccessEvent> {
    override fun deserialize(topic: String?, data: ByteArray?): UploadFileSuccessEvent? =
        data?.let { objectMapper.readValue<UploadFileSuccessEvent>(it) }
}
class UploadFileFailureEventDeserializer: Deserializer<UploadFileFailureEvent> {
    override fun deserialize(topic: String?, data: ByteArray?): UploadFileFailureEvent? =
        data?.let { objectMapper.readValue<UploadFileFailureEvent>(it) }
}

