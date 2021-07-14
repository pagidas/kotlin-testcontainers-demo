package org.example.demo

import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.http4k.core.Status
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.time.Instant
import java.util.*
import java.util.concurrent.CompletableFuture

class UploadFileEventProcessor(private val fileIOApi: FileIOApi, private val s3Config: S3Config, private val s3: S3AsyncClient)
    : Processor<Long, UploadFileEvent, Long, UploadFileEventBase> {

    private val log = LoggerFactory.getLogger(this::class.java)
    private lateinit var context: ProcessorContext<Long, UploadFileEventBase>

    override fun init(context: ProcessorContext<Long, UploadFileEventBase>) {
        this.context = context
        log.info("Processor initialised")
    }

    override fun process(record: Record<Long, UploadFileEvent>) {
        val event = record.value()
        log.info("Attempt to download file from file.io with event: $event")
        val response = fileIOApi.downloadFile(event.fileIoKey)

        if (response.status == Status.OK) {
            val content = response.bodyString()
            s3.uploadFileIoContent(event.id, content).get().let { outcome ->
                if (outcome.isSuccessful) {
                    val successEvent = UploadFileSuccessEvent(event.id, outcome.s3Tag ?: "")
                    context.forwardToSuccessSink(successEvent, record)
                } else {
                    val failureEvent = UploadFileFailureEvent(event.id, outcome.cause?.message ?: "")
                    context.forwardToFailureSink(failureEvent, record)
                }
            }
        } else {
            val failureEvent = UploadFileFailureEvent(event.id, "File with id: ${event.fileIoKey} is not found in FileIO")
            context.forwardToFailureSink(failureEvent, record)
        }
    }

    override fun close() {
        log.info("Closing stream processor")
    }

    private fun ProcessorContext<Long, UploadFileEventBase>.forwardToSuccessSink(event: UploadFileSuccessEvent, record: Record<Long, UploadFileEvent>) {
        log.info("Forwarding success event: $event to: $SINK_SUCCESS_NODE")
        forward(record.withValue(event).withTimestamp(Instant.now().epochSecond), SINK_SUCCESS_NODE)
    }

    private fun ProcessorContext<Long, UploadFileEventBase>.forwardToFailureSink(event: UploadFileFailureEvent, record: Record<Long, UploadFileEvent>)  {
        log.info("Forwarding failure event: $event to: $SINK_FAILURE_NODE")
        forward(record.withValue(event).withTimestamp(Instant.now().epochSecond), SINK_FAILURE_NODE)
    }

    private fun S3AsyncClient.uploadFileIoContent(key: UUID, content: String): CompletableFuture<S3FileUploadOutcome> {
        val request = PutObjectRequest.builder().run {
            bucket(s3Config.bucket)
            key(key.toString())
            build()
        }

        return putObject(request, AsyncRequestBody.fromString(content))
            .thenApply {
                log.debug("Got response from S3: $it")
                S3FileUploadOutcome(it.sdkHttpResponse().isSuccessful, it.eTag(), null)
            }
            .exceptionally {
                log.debug("Got exception while uploading to S3: $it")
                S3FileUploadOutcome(false, null, it.cause)
            }
    }
}

// internals //
data class S3FileUploadOutcome(val isSuccessful: Boolean, val s3Tag: String?, val cause: Throwable?)


