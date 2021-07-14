package org.example.demo

import org.http4k.client.ApacheClient
import org.http4k.core.*
import org.http4k.filter.ClientFilters
import org.http4k.filter.RequestFilters
import org.http4k.filter.ResponseFilters
import org.slf4j.LoggerFactory

class FileIOApi(config: FileIoApiConfig) {

    private val log = LoggerFactory.getLogger(this::class.java)

    private val httpClient: HttpHandler =
        ClientFilters.SetBaseUriFrom(Uri.of(config.uri))
            .then(RequestFilters.Tap { log.debug("Sending request $it") })
            .then(ResponseFilters.Tap { log.debug("Got response $it") })
            .then(ApacheClient())

    fun downloadFile(key: String): Response = httpClient(Request(Method.GET, "/$key"))
}