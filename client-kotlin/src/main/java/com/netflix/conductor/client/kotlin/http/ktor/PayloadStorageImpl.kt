package com.netflix.conductor.client.kotlin.http.ktor

import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.client.kotlin.http.PayloadStorage
import com.netflix.conductor.common.run.ExternalStorageLocation
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.request.*
import io.ktor.client.statement.*

class PayloadStorageImpl(val rootURI: String, private val httpClient: HttpClient): PayloadStorage {
    override suspend fun getLocation(
        operation: PayloadStorage.Operation?,
        payloadType: PayloadStorage.PayloadType?,
        path: String?
    ): ExternalStorageLocation {
        val uri: String = when (payloadType) {
            PayloadStorage.PayloadType.WORKFLOW_INPUT, PayloadStorage.PayloadType.WORKFLOW_OUTPUT -> "workflow"
            PayloadStorage.PayloadType.TASK_INPUT, PayloadStorage.PayloadType.TASK_OUTPUT -> "tasks"
            else -> throw ConductorClientException("Invalid payload type: $payloadType for operation: $operation")
        }
        val response = httpClient.get {
            url("$rootURI/$uri/externalstoragelocation")
            parameter("path", path)
            parameter("operation", operation)
            parameter("payloadType", payloadType)
        }
        return response.body()
    }

    override suspend fun upload(path: String, payload: ByteArray, payloadSize: Long) {
        httpClient.put {
            url("$rootURI/$path")
            setBody(payload)
        }
    }

    override suspend fun download(path: String): ByteArray {
        val response = httpClient.get {
            url("$rootURI/$path")
        }
        return response.readBytes()
    }
}