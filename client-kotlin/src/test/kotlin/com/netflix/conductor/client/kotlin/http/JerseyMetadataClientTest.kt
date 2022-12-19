package com.netflix.conductor.client.kotlin.http

import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import groovy.test.GroovyTestCase.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.Mockito

class JerseyMetadataClientTest : JerseyClientTest() {

    private var metadataClient: JerseyMetadataClient = JerseyMetadataClient(requestHandler)

    @BeforeEach
    fun setup() {
        metadataClient.setRootURI(ROOT_URL)
    }

    @Test
    fun workflowDelete(): Unit = runBlocking {
        val workflowName = "test"
        val version = 1
        val uri = createURI("metadata/workflow/$workflowName/$version")

        metadataClient.unregisterWorkflowDef(workflowName, version)

        Mockito.verify(requestHandler, Mockito.times(1))
            .delete(uri, null)
    }

    @Test
    fun workflowDeleteThrowsException(): Unit = runBlocking {
        val workflowName = "test"
        val version = 1
        val uri = createURI("metadata/workflow/$workflowName/$version")

        Mockito.`when`(requestHandler.delete(uri, null)).thenThrow(RuntimeException())

        val exception = assertThrows<ConductorClientException> {
            runBlocking{
                metadataClient.unregisterWorkflowDef(workflowName, version)
            }
        }
        assertEquals("Unable to invoke Conductor API with uri: $uri, runtime exception occurred", exception.message)

        Mockito.verify(requestHandler, Mockito.times(1))
            .delete(uri, null)
    }

    @Test
    fun workflowDeleteNameMissing(): Unit = runBlocking {
        assertThrows<IllegalArgumentException> {
            runBlocking{
                metadataClient.unregisterWorkflowDef("   ", 1)
            }
        }
    }
}