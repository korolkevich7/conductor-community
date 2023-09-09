package com.netflix.conductor.client.kotlin.http.ktor

import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.client.kotlin.http.MetadataClient
import io.ktor.client.engine.mock.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.Test
import kotlin.test.BeforeTest

class KtorMetadataClientTest : KtorClientTest() {

    private lateinit var metadataClient: MetadataClient

    @BeforeTest
    fun setup() {
        val mockEngine = MockEngine { request ->
            when (request.url.toString()) {
                "$ROOT_URL/metadata/workflow/test/1" -> respond(
                    content = "$ROOT_URL/event",
                    headers = headersOf(HttpHeaders.ContentType, "application/json")
                )
                "$ROOT_URL/metadata/workflow/test/2" -> throw RuntimeException()
                else -> throw IllegalArgumentException("Wrong url")
            }

        }
        metadataClient =  KtorMetadataClient(ROOT_URL, httpClient(mockEngine))
    }

    @Test
    fun workflowDelete(): Unit = runBlocking {
        val workflowName = "test"
        val version = 1
        metadataClient.unregisterWorkflowDef(workflowName, version)
    }


    @Test
    fun workflowDeleteThrowsException(): Unit = runBlocking {
        val workflowName = "test"
        val version = 2
        val uri = "${ROOT_URL}/metadata/workflow/$workflowName/$version"

        val exception = assertFailsWith<ConductorClientException> {
            runBlocking {
                metadataClient.unregisterWorkflowDef(workflowName, version)
            }
        }
        assertEquals("Unable to invoke Conductor API with uri: $uri, runtime exception occurred", exception.message)
    }

    @Test
    fun workflowDeleteNameMissing(): Unit = runBlocking {
        assertFailsWith<IllegalArgumentException> {
            runBlocking {
                metadataClient.unregisterWorkflowDef("   ", 1)
            }
        }
    }
}