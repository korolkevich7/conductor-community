package com.netflix.conductor.client.kotlin.http.ktor

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.netflix.conductor.common.jackson.JsonProtoModule
import io.ktor.client.*
import io.ktor.client.engine.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.serialization.jackson.*
import java.net.URI


const val ROOT_URL = "http://localhost/dummyroot"
abstract class KtorClientTest {

//    protected val requestHandler: JerseyClientRequestHandler = mock()
    protected val objectMapper = jacksonObjectMapper()

    init {
        objectMapper.configureObjectMapper()
    }
    fun createURI(path: String): URI = URI.create(ROOT_URL + path)

    fun httpClient(engine: HttpClientEngine): HttpClient = defaultHttpClient(engine)
}