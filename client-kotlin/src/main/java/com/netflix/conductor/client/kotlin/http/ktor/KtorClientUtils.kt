package com.netflix.conductor.client.kotlin.http.ktor

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.common.jackson.JsonProtoModule
import io.ktor.client.*
import io.ktor.client.engine.*
import io.ktor.client.engine.okhttp.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.serialization.jackson.*

fun defaultHttpClient(engine: HttpClientEngine): HttpClient {
    return HttpClient(engine) {
        configureClient()
    }
}

fun defaultHttpClient(): HttpClient {
    return HttpClient(OkHttp) {
        configureClient()
    }
}

public fun HttpClientConfig<*>.configureClient() {
    expectSuccess = true
    HttpResponseValidator {
        handleResponseExceptionWithRequest { exception, request ->
            throw ConductorClientException("Unable to invoke Conductor API with uri: ${request.url}, runtime exception occurred", exception)


            //TODO: handle another exceptions
//            val clientException = exception as? ClientRequestException ?: return@handleResponseExceptionWithRequest
//            val exceptionResponse = clientException.response
//            throw ConductorClientException("Unable to invoke Conductor API with uri: ${request.url}, runtime exception occurred", exception)
//                    if (exceptionResponse.status == HttpStatusCode.NotFound) {
//                        val exceptionResponseText = exceptionResponse.bodyAsText()
//                        throw MissingPageException(exceptionResponse, exceptionResponseText)
//                    }


        }
    }
    install(ContentNegotiation) {
        jackson {
            configureObjectMapper()
        }
    }
}

fun ObjectMapper.configureObjectMapper() {
    registerModule(JsonProtoModule())
    configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)
    configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false)
    configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    setDefaultPropertyInclusion(
        JsonInclude.Value.construct(
            JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_EMPTY
        ))
    setSerializationInclusion(JsonInclude.Include.ALWAYS)
}