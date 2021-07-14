package org.example.demo

import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import software.amazon.awssdk.services.s3.S3AsyncClient

class KafkaStreamApp(private val kafkaConfig: KafkaConfig,
                     private val fileIOApi: FileIOApi,
                     private val s3Config: S3Config,
                     s3ClientFactory: S3ClientFactory) {

    private val s3: S3AsyncClient = s3ClientFactory.getClient()
    private val streamApp: KafkaStreams = KafkaStreams(StreamsBuilder().build().run {

            addSource(SOURCE_NODE,
                LongDeserializer(),
                UploadFileEventDeserializer(),
                UploadFileEvent.topic)

            addProcessor(PROCESSOR_NODE,
                ProcessorSupplier { UploadFileEventProcessor(fileIOApi, s3Config, s3) },
                SOURCE_NODE)

            addSink(SINK_SUCCESS_NODE,
                UploadFileSuccessEvent.topic,
                LongSerializer(),
                UploadFileEventBaseSerializer(),
                PROCESSOR_NODE)

            addSink(SINK_FAILURE_NODE,
                UploadFileFailureEvent.topic,
                LongSerializer(),
                UploadFileEventBaseSerializer(),
                PROCESSOR_NODE)

    }, this.kafkaConfig.toProperties())

    fun start() = streamApp.start()

    fun close() = streamApp.close()

    fun isRunning(): Boolean = streamApp.state().isValidTransition(KafkaStreams.State.RUNNING)
}

// serdes //
class UploadFileEventBaseSerializer: Serializer<UploadFileEventBase> {
    override fun serialize(topic: String?, data: UploadFileEventBase?): ByteArray? =
        when(data) {
            is UploadFileEvent -> objectMapper.writeValueAsBytes(data)
            is UploadFileSuccessEvent -> objectMapper.writeValueAsBytes(data)
            is UploadFileFailureEvent -> objectMapper.writeValueAsBytes(data)
            // should never happen
            else -> null
        }
}
class UploadFileEventDeserializer: Deserializer<UploadFileEvent> {
    override fun deserialize(topic: String?, data: ByteArray?): UploadFileEvent? =
        data?.let { objectMapper.readValue<UploadFileEvent>(it) }
}

const val SOURCE_NODE = "upload-file-source"
const val PROCESSOR_NODE = "upload-file-processor"
const val SINK_SUCCESS_NODE = "upload-file-sink-success"
const val SINK_FAILURE_NODE = "upload-file-sink-failure"
