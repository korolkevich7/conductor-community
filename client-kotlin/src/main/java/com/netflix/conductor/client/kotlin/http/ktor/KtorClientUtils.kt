package com.netflix.conductor.client.kotlin.http.ktor

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.conductor.client.kotlin.config.ObjectMapperProvider
import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.common.jackson.JsonProtoModule
import com.netflix.conductor.common.validation.ErrorResponse
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.*
import io.ktor.client.engine.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.statement.*
import io.ktor.serialization.jackson.*

val logger = KotlinLogging.logger { }

fun defaultHttpClient(engine: HttpClientEngine): HttpClient {
    return HttpClient(engine) {
        configureClient()
    }
}

fun HttpClientConfig<*>.configureClient() {
    expectSuccess = true
    HttpResponseValidator {
        handleResponseExceptionWithRequest { exception, request ->
            try {
                val clientException = exception as? ResponseException
                    ?: throw ConductorClientException("Unable to invoke Conductor API with uri: ${request.url}, runtime exception occurred", exception)
                val exceptionResponse = clientException.response
                val errorMessage = exceptionResponse.bodyAsText()
                logger.warn { "Unable to invoke Conductor API with uri: ${request.url}, unexpected response from server: status=${exceptionResponse.status}, responseBody='$errorMessage'." }

                runCatching { ObjectMapperProvider.objectMapper.readValue<ErrorResponse>(errorMessage) }
                    .onSuccess { throw ConductorClientException(exceptionResponse.status.value, it) }
                    .onFailure { throw ConductorClientException(exceptionResponse.status.value, errorMessage) }

            } catch (e: Exception) {
                throw ConductorClientException("Unable to invoke Conductor API with uri: ${request.url}, runtime exception occurred", exception)
            }
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