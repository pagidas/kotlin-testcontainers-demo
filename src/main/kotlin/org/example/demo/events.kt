package org.example.demo

import java.util.*

open class UploadFileEventBase
data class UploadFileEvent(val id: UUID, val fileIoKey: String): UploadFileEventBase() {
    companion object {
        const val topic = "upload-file-events"
    }
}
data class UploadFileSuccessEvent(val id: UUID, val s3Tag: String): UploadFileEventBase() {
    companion object {
        const val topic = "upload-file-success-events"
    }
}
data class UploadFileFailureEvent(val id: UUID, val reason: String): UploadFileEventBase() {
    companion object {
        const val topic = "upload-file-failure-events"
    }
}
