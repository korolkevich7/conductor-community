package com.netflix.conductor.client.kotlin.http.ktor

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.conductor.client.kotlin.config.DefaultConductorClientConfiguration
import com.netflix.conductor.client.kotlin.config.ObjectMapperProvider
import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.client.kotlin.http.PayloadStorage
import com.netflix.conductor.common.run.ExternalStorageLocation
import io.ktor.client.*
import io.ktor.client.engine.okhttp.*
import okhttp3.ConnectionPool
import okhttp3.OkHttpClient
import org.slf4j.LoggerFactory
import java.io.IOException
import java.time.Duration
import java.util.concurrent.TimeUnit

open class KtorBaseClient(open val rootURI: String, val httpClient: HttpClient) {
    constructor(rootURI: String) : this(rootURI, createOkHttpClient() )

    private val payloadStorage: PayloadStorage = PayloadStorageImpl(rootURI, httpClient)

    val conductorClientConfiguration = DefaultConductorClientConfiguration
    val objectMapper: ObjectMapper = ObjectMapperProvider.objectMapper

    internal suspend fun downloadFromExternalStorage(
        payloadType: PayloadStorage.PayloadType, path: String
    ): Map<String, Any> {
        require(path.isNotBlank()) { "uri cannot be blank" }
        val externalStorageLocation: ExternalStorageLocation = payloadStorage.getLocation(
            PayloadStorage.Operation.READ, payloadType, path
        )
        try {
            val typeRef = object : TypeReference<MutableMap<String, Any>>() {}
            val bytes = payloadStorage.download(externalStorageLocation.uri)
            return objectMapper.readValue(bytes, typeRef)
        } catch (e: IOException) {
            val errorMsg = "Unable to download payload from external storage location: $path"
            LOGGER.error(errorMsg, e)
            throw ConductorClientException(errorMsg, e)
        }
    }

    internal suspend fun uploadToPayloadStorage(
        payloadType: PayloadStorage.PayloadType, payloadBytes: ByteArray, payloadSize: Long
    ): String {
        require(
            payloadType == PayloadStorage.PayloadType.WORKFLOW_INPUT ||
                    payloadType == PayloadStorage.PayloadType.TASK_OUTPUT
        ) { "Payload type must be workflow input or task output" }
        
        val externalStorageLocation: ExternalStorageLocation =
            payloadStorage.getLocation(PayloadStorage.Operation.WRITE, payloadType, "")
        payloadStorage.upload(
            externalStorageLocation.uri,
            payloadBytes,
            payloadSize
        )
        return externalStorageLocation.path
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(KtorBaseClient::class.java)
    }
}

fun createOkHttpClient(): HttpClient {
    return HttpClient(OkHttp){
        engine {
            pipelining = true
            preconfigured = OkHttpClient.Builder()
                .connectionPool(ConnectionPool(100, 10, TimeUnit.SECONDS))
                .readTimeout(Duration.ofSeconds(30))
                .build()
        }
    }
}