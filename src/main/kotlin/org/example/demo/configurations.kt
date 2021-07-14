package org.example.demo

import com.typesafe.config.Config
import org.apache.kafka.streams.StreamsConfig

class KafkaConfig(config: Config) {
    val appId: String = config.getString("kafka.appId")
    val bootstrapServers: String = config.getString("kafka.bootstrapServers")

    fun toProperties() = mapOf(
        StreamsConfig.APPLICATION_ID_CONFIG to appId,
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
    ).toProperties()
}

class FileIoApiConfig(config: Config) {
    val uri: String = config.getString("fileIo.uri")
}

class S3Config(config: Config) {
    val uri: String = config.getString("s3.uri")
    val region: String = config.getString("s3.region")
    val bucket: String = config.getString("s3.bucket")
    val accessKey: String = config.getString("s3.accessKey")
    val secretKey: String = config.getString("s3.secretKey")
}
