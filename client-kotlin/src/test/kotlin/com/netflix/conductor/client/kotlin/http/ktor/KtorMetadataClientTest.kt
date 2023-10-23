package com.netflix.conductor.client.kotlin.http.ktor

import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.client.kotlin.http.MetadataClient
import com.netflix.conductor.common.metadata.tasks.TaskDef
import com.netflix.conductor.common.metadata.workflow.WorkflowDef

import com.fasterxml.jackson.module.kotlin.*
import io.ktor.client.engine.mock.*
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class KtorMetadataClientTest : KtorClientTest() {

    private lateinit var metadataClient: MetadataClient

    @BeforeTest
    fun setup() {
        val workflowDef = WorkflowDef()
        workflowDef.name = "testName"

        val taskDef = TaskDef()
        taskDef.name = "taskTestName"
        val mockEngine = MockEngine { request ->
            when (request.url.toString()) {
                "$ROOT_URL/metadata/workflow/${workflowDef.name}" -> respond(
                    content = objectMapper.writeValueAsString(workflowDef),
                    headers = headersOf(HttpHeaders.ContentType, "application/json")
                )
                "$ROOT_URL/metadata/taskdefs/${taskDef.name}" -> respond(
                    content = objectMapper.writeValueAsString(taskDef),
                    headers = headersOf(HttpHeaders.ContentType, "application/json")
                )
                "$ROOT_URL/metadata/workflow" -> respond(
                    content = "",
                    status = HttpStatusCode.NoContent,
                    headers = headersOf(HttpHeaders.ContentType, "application/json")
                )
                "$ROOT_URL/metadata/taskdefs" -> respond(
                    content = "",
                    status = HttpStatusCode.NoContent,
                    headers = headersOf(HttpHeaders.ContentType, "application/json")
                )
                "$ROOT_URL/metadata/taskdefs/task_1" -> respond(
                    content = "",
                    status = HttpStatusCode.NoContent
                )
                "$ROOT_URL/metadata/workflow/validate" -> respond(
                    content = "",
                    status = HttpStatusCode.OK
                )
                else -> throw IllegalArgumentException("Wrong url")
            }

        }
        metadataClient =  KtorMetadataClient(ROOT_URL, httpClient(mockEngine))
    }

    @Test
    fun unregisterWorkflowDef(): Unit = runBlocking {
        val workflowName = "testWorkflow"
        val version = 1
        val metadataClient = ktorMetadataClientWithMock { request ->
            assertContains(request.url.toString(), workflowName)
            respond(
                content = "$ROOT_URL/event",
                headers = headersOf(HttpHeaders.ContentType, "application/json")
            )
        }

        metadataClient.unregisterWorkflowDef(workflowName, version)
    }


    @Test
    fun unregisterWorkflowDefThrowsException(): Unit = runBlocking {
        val workflowName = "testWorkflow"
        val version = 2
        val uri = "${ROOT_URL}/metadata/workflow/$workflowName/$version"

        val metadataClient = ktorMetadataClientWithMock { request ->
            assertEquals(uri, request.url.toString())
            throw RuntimeException()
        }

        val exception = assertFailsWith<ConductorClientException> {
            runBlocking {
                metadataClient.unregisterWorkflowDef(workflowName, version)
            }
        }
        assertEquals("Unable to invoke Conductor API with uri: $uri, runtime exception occurred", exception.message)
    }

    @Test
    fun unregisterWorkflowDefNameMissing(): Unit = runBlocking {
        val metadataClient = ktorMetadataClientWithMock()
        assertFailsWith<IllegalArgumentException> {
            runBlocking {
                metadataClient.unregisterWorkflowDef("   ", 1)
            }
        }
    }

    @Test
    fun updateWorkflowDefs(): Unit = runBlocking {

        val metadataClient = ktorMetadataClientWithMock { request ->
            val workflows = objectMapper.readValue<List<WorkflowDef>>(request.body.toByteArray())
            assertEquals("wf1", workflows.first().name)
            assertEquals("wf2", workflows.last().name)
            respond(
                content = "",
                status = HttpStatusCode.NoContent,
                headers = headersOf(HttpHeaders.ContentType, "application/json")
            )
        }

        metadataClient.updateWorkflowDefs(listOf(WorkflowDef("wf1"), WorkflowDef("wf2")))
    }

    @Test
    fun getWorkflowDef(): Unit = runBlocking {
        val workflowDefName = "testName"
        val workflowDef = metadataClient.getWorkflowDef(workflowDefName)
        assertEquals(workflowDefName, workflowDef.name)
    }

    @Test
    fun updateTaskDef(): Unit = runBlocking {
        metadataClient.updateTaskDef(TaskDef())
    }

    @Test
    fun getTaskDef(): Unit = runBlocking {
        val taskDefName = "taskTestName"
        val taskDef = metadataClient.getTaskDef(taskDefName)
        assertEquals(taskDefName, taskDef.name)
    }

    @Test
    fun validateWorkflowDef(): Unit = runBlocking {
        metadataClient.validateWorkflowDef(WorkflowDef())
    }

    @Test
    fun registerWorkflowDef(): Unit = runBlocking {
        metadataClient.registerWorkflowDef(WorkflowDef())
    }

    @Test
    fun registerTaskDefs(): Unit = runBlocking {
        metadataClient.registerTaskDefs(listOf(TaskDef(), TaskDef()))
    }

    @Test
    fun unregisterTaskDef(): Unit = runBlocking {
        metadataClient.unregisterTaskDef("task_1")
    }

    private fun ktorMetadataClientWithMock() = ktorMetadataClientWithMock { respond("") }
    private fun ktorMetadataClientWithMock(handler: suspend MockRequestHandleScope.(HttpRequestData) -> HttpResponseData): MetadataClient {
        return KtorMetadataClient(ROOT_URL, httpClient(MockEngine.invoke(handler)))
    }

    private fun WorkflowDef(name: String): WorkflowDef {
        val workflowDef = WorkflowDef()
        workflowDef.name = name
        return workflowDef
    }
}