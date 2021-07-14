package org.example.demo

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import java.net.URI

class S3ClientFactory(private val config: S3Config) {

    fun getClient(): S3AsyncClient = S3AsyncClient.builder().run {
        endpointOverride(URI.create(config.uri))
        region(Region.of(config.region))
        credentialsProvider(
            StaticCredentialsProvider.create(
            AwsBasicCredentials.create(config.accessKey, config.secretKey)))
        build()
    }
}