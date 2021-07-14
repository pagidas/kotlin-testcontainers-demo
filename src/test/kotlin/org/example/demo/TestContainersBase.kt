package org.example.demo

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.mockserver.client.MockServerClient
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.mockserver.model.MediaType
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.MockServerContainer
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import java.net.URI

private val log = LoggerFactory.getLogger(TestContainersBase::class.java)

open class TestContainersBase {
    companion object {
        private const val KAFKA_CONTAINER_PORT = 9093
        private const val MOCK_SERVER_CONTAINER_PORT = 1080
        private const val S3_MOCK_CONTAINER_PORT = 9090

        private val kafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3")).apply {
            withExposedPorts(KAFKA_CONTAINER_PORT)
        }
        private val mockServerContainer = MockServerContainer(DockerImageName.parse("mockserver/mockserver")).apply {
            withExposedPorts(MOCK_SERVER_CONTAINER_PORT)
        }
        private val s3MockContainer = GenericContainer<Nothing>(DockerImageName.parse("adobe/s3mock")).apply {
            withExposedPorts(S3_MOCK_CONTAINER_PORT)
            withEnv("initialBuckets", "file-uploads")
        }

        val mockFileIo: MockServerClient
        val testS3Client: S3Client

        init {
            // init containers
            kafkaContainer.start()
            mockServerContainer.start()
            s3MockContainer.start()

            // containers' dynamic hosts and ports
            val kafkaBootstrapServers = "localhost:${kafkaContainer.firstMappedPort}"
            val fileIoUri = "http://${mockServerContainer.host}:${mockServerContainer.serverPort}"
            val s3Uri = URI.create("http://localhost:${s3MockContainer.firstMappedPort}")

            // init context
            setupKafka(kafkaBootstrapServers, UploadFileEvent.topic, UploadFileSuccessEvent.topic)
            mockFileIo = setupMockFileIo(mockServerContainer.host, mockServerContainer.serverPort)
            testS3Client = setupS3(s3Uri)
            setupEnv(kafkaBootstrapServers, fileIoUri, s3Uri)
        }
    }
}

// internals //
private fun setupEnv(kafkaBootstrapServers: String, fileIoUri: String, endpoint: URI) {
    System.setProperty("KAFKA_BOOTSTRAP_SERVERS", kafkaBootstrapServers)
    System.setProperty("FILE_IO_URI", fileIoUri)
    System.setProperty("S3_URI", endpoint.toASCIIString())
    System.setProperty("S3_ACCESS_KEY", "some-access-key")
    System.setProperty("S3_SECRET_KEY", "some-secret-key")
}

private fun setupS3(endpoint: URI): S3Client =
    S3Client.builder().run {
        endpointOverride(endpoint)
        region(Region.EU_WEST_2)
        credentialsProvider(
            StaticCredentialsProvider.create(
            AwsBasicCredentials.create("some-access-key", "some-secret-key")))
        build()
    }

private fun setupKafka(bootstrapServers: String, vararg topics: String) {
    val admin = AdminClient.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers))
    try {
        admin.createTopics(topics.map { NewTopic(it, 1, 1) })
    } catch (e: Exception) {
        log.error("Error creating kafka topics in testcontainers: $e")
    }
    admin.listTopics().listings().get().forEach {
        log.info("Created kafka topic: $it")
    }
}

private fun setupMockFileIo(host: String, port: Int) = MockServerClient(host, port).apply {
        // basic expectations, but returns the client so new stubs can be added when needed directly on tests
        `when`(request()
            .withMethod("GET")
            .withPath("/a-valid-key"))
        .respond(response()
            .withStatusCode(200)
            .withContentType(MediaType.TEXT_PLAIN)
            .withBody("hello!"))

        `when`(request()
            .withMethod("GET")
            .withPath("/an-invalid-key"))
        .respond(response()
            .withStatusCode(302)
            .withContentType(MediaType.TEXT_PLAIN)
            .withBody("Found. Redirecting to https://www.file.io/deleted"))
}
