package com.netflix.conductor.client.kotlin.http.ktor

import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.client.kotlin.http.PayloadStorage
import com.netflix.conductor.client.kotlin.http.WorkflowClient
import com.netflix.conductor.client.kotlin.telemetry.*
import com.netflix.conductor.common.metadata.workflow.RerunWorkflowRequest
import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest
import com.netflix.conductor.common.model.BulkResponse
import com.netflix.conductor.common.run.SearchResult
import com.netflix.conductor.common.run.Workflow
import com.netflix.conductor.common.run.WorkflowSummary
import com.netflix.conductor.common.utils.ExternalPayloadStorage
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.utils.io.errors.*

class KtorWorkflowClient(rootURI: String, httpClient: HttpClient): WorkflowClient, KtorBaseClient(rootURI, httpClient) {

    override suspend fun startWorkflow(startWorkflowRequest: StartWorkflowRequest): String {
        require(startWorkflowRequest.name.isNotBlank()) { "Workflow name cannot be null or empty" }
        require(startWorkflowRequest.externalInputPayloadStoragePath.isBlank()) { "External Storage Path must not be set" }

        populateWorkflowInput(startWorkflowRequest)
        val response = httpClient.post {
            url("$rootURI/workflow")
            setBody(startWorkflowRequest)
            contentType(ContentType.Application.Json)
        }
        return response.body()
    }

    override suspend fun getWorkflow(workflowId: String, includeTasks: Boolean): Workflow {
        require(workflowId.isNotBlank()) { "workflow id cannot be blank" }
        val response = httpClient.get {
            url("$rootURI/workflow/$workflowId")
            parameter("includeTasks", includeTasks)
        }
        val workflow: Workflow = response.body()
        populateWorkflowOutput(workflow)
        return workflow
    }

    override suspend fun getWorkflows(
        name: String,
        correlationId: String,
        includeClosed: Boolean,
        includeTasks: Boolean
    ): List<Workflow> {
        require(name.isNotBlank()) { "name cannot be blank" }
        require(correlationId.isNotBlank()) { "correlationId cannot be blank" }

        val response = httpClient.get {
            url("$rootURI/workflow/$name/correlated/$correlationId")
            parameter("includeTasks", includeTasks)
            parameter("includeClosed", includeClosed)
        }
        val workflows: List<Workflow> = response.body()
        workflows.forEach { populateWorkflowOutput(it) }
        return workflows
    }

    override suspend fun deleteWorkflow(workflowId: String, archiveWorkflow: Boolean) {
        require(workflowId.isNotBlank()) { "workflow id cannot be blank" }
        httpClient.delete {
            url("$rootURI/workflow/$workflowId/remove")
            parameter("archiveWorkflow", archiveWorkflow)
        }
    }

    override suspend fun terminateWorkflows(workflowIds: List<String>, reason: String): BulkResponse? {
        require(workflowIds.isNotEmpty()) { "workflow ids cannot be empty" }
        val response = httpClient.post {
            url("$rootURI/workflow/bulk/terminate")
            parameter("reason", reason)
            setBody(workflowIds)
            contentType(ContentType.Application.Json)
        }
        return response.body()
    }

    override suspend fun getRunningWorkflow(workflowName: String, version: Int): List<String> {
        require(workflowName.isNotBlank()) { "Workflow name cannot be blank" }
        val response = httpClient.get {
            url("$rootURI/workflow/running/$workflowName")
            parameter("version", version)
        }
        return response.body()
    }

    override suspend fun getWorkflowsByTimePeriod(
        workflowName: String,
        version: Int,
        startTime: Long,
        endTime: Long
    ): List<String> {
        require(workflowName.isNotBlank()) { "Workflow name cannot be blank" }
        val response = httpClient.get {
            url("$rootURI/workflow/running/$workflowName")
            parameter("version", version)
            parameter("startTime", startTime)
            parameter("endTime", endTime)
        }
        return response.body()
    }

    override suspend fun runDecider(workflowId: String) {
        require(workflowId.isNotBlank()) { "workflow id cannot be blank" }
        httpClient.post {
            url("$rootURI/workflow/decide/$workflowId")
        }
    }

    override suspend fun pauseWorkflow(workflowId: String) {
        require(workflowId.isNotBlank()) { "workflow id cannot be blank" }
        httpClient.put {
            url("$rootURI/workflow/$workflowId/pause")
        }
    }

    override suspend fun resumeWorkflow(workflowId: String) {
        require(workflowId.isNotBlank()) { "workflow id cannot be blank" }
        httpClient.put {
            url("$rootURI/workflow/$workflowId/resume")
        }
    }

    override suspend fun skipTaskFromWorkflow(workflowId: String, taskReferenceName: String) {
        require(workflowId.isNotBlank()) { "workflow id cannot be blank" }
        require(taskReferenceName.isNotBlank()) { "Task reference name cannot be blank" }
        httpClient.put {
            url("$rootURI/workflow/$workflowId/skiptask/$taskReferenceName")
        }
    }

    override suspend fun rerunWorkflow(workflowId: String, rerunWorkflowRequest: RerunWorkflowRequest): String {
        require(workflowId.isNotBlank()) { "workflow id cannot be blank" }
        val response = httpClient.post {
            url("$rootURI/workflow/$workflowId/rerun")
            setBody(rerunWorkflowRequest)
            contentType(ContentType.Application.Json)
        }
        return response.body()
    }

    override suspend fun restart(workflowId: String, useLatestDefinitions: Boolean) {
        require(workflowId.isNotBlank()) { "workflow id cannot be blank" }
        httpClient.post {
            url("$rootURI/workflow/$workflowId/restart")
            parameter("useLatestDefinitions", useLatestDefinitions)
        }
    }

    override suspend fun retryLastFailedTask(workflowId: String) {
        require(workflowId.isNotBlank()) { "workflow id cannot be blank" }
        httpClient.post {
            url("$rootURI/workflow/$workflowId/retry")
        }
    }

    override suspend fun resetCallbacksForInProgressTasks(workflowId: String) {
        require(workflowId.isNotBlank()) { "workflow id cannot be blank" }
        httpClient.post {
            url("$rootURI/workflow/$workflowId/resetcallbacks")
        }
    }

    override suspend fun terminateWorkflow(workflowId: String, reason: String) {
        require(workflowId.isNotBlank()) { "workflow id cannot be blank" }
        httpClient.delete {
            url("$rootURI/workflow/$workflowId")
            parameter("reason", reason)
        }
    }

    override suspend fun search(query: String): SearchResult<WorkflowSummary> {
        val response = httpClient.get {
            url("$rootURI/workflow/search")
            parameter("query", query)
        }
        return response.body()
    }

    override suspend fun search(
        start: Int,
        size: Int,
        sort: String,
        freeText: String,
        query: String
    ): SearchResult<WorkflowSummary> {
        val response = httpClient.get {
            url("$rootURI/workflow/search")
            parameter("start", start)
            parameter("size", size)
            parameter("sort", sort)
            parameter("freeText", freeText)
            parameter("query", query)
        }
        return response.body()
    }

    override suspend fun searchV2(query: String): SearchResult<Workflow> {
        val response = httpClient.get {
            url("$rootURI/workflow/search-v2")
            parameter("query", query)
        }
        return response.body()
    }

    override suspend fun searchV2(
        start: Int,
        size: Int,
        sort: String,
        freeText: String,
        query: String
    ): SearchResult<Workflow> {
        val response = httpClient.get {
            url("$rootURI/workflow/search-v2")
            parameter("start", start)
            parameter("size", size)
            parameter("sort", sort)
            parameter("freeText", freeText)
            parameter("query", query)
        }
        return response.body()
    }

    private suspend fun populateWorkflowOutput(workflow: Workflow) {
        if (workflow.externalOutputPayloadStoragePath?.isNotBlank() == true) {
            MetricsContainer.incrementExternalPayloadUsedCount(
                workflow.workflowName,
                PayloadStorage.Operation.READ.name,
                PayloadStorage.PayloadType.WORKFLOW_OUTPUT.name
            )
            workflow.output = downloadFromExternalStorage(
                PayloadStorage.PayloadType.WORKFLOW_OUTPUT,
                workflow.externalOutputPayloadStoragePath
            )
        }
    }

    private suspend fun populateWorkflowInput(startWorkflowRequest: StartWorkflowRequest) {
        //TODO add exception handler
        val version = startWorkflowRequest.version?.toString() ?: "latest"

        val workflowInputBytes = objectMapper.writeValueAsBytes(startWorkflowRequest.input)
        val workflowInputSize = workflowInputBytes.size.toLong()
        MetricsContainer.recordWorkflowInputPayloadSize(
            startWorkflowRequest.name, version, workflowInputSize
        )
        try {
            if (workflowInputSize
                > conductorClientConfiguration.workflowInputPayloadThresholdKB * 1024L
            ) {
                if (!conductorClientConfiguration.isExternalPayloadStorageEnabled
                    || (workflowInputSize
                            > conductorClientConfiguration.workflowInputMaxPayloadThresholdKB
                            * 1024L)
                ) {
                    val errorMsg = "Input payload larger than the allowed threshold of:" +
                            " ${conductorClientConfiguration.workflowInputPayloadThresholdKB} KB"
                    throw ConductorClientException(errorMsg)
                }
                MetricsContainer.incrementExternalPayloadUsedCount(
                    startWorkflowRequest.name,
                    ExternalPayloadStorage.Operation.WRITE.name,
                    ExternalPayloadStorage.PayloadType.WORKFLOW_INPUT.name
                )
                val externalStoragePath: String = uploadToPayloadStorage(
                    PayloadStorage.PayloadType.WORKFLOW_INPUT,
                    workflowInputBytes,
                    workflowInputSize
                )
                startWorkflowRequest.externalInputPayloadStoragePath = externalStoragePath
                startWorkflowRequest.input = null
            }
        } catch (e: IOException) {
            MetricsContainer.incrementWorkflowStartErrorCount(
                startWorkflowRequest.name,
                e
            )
            throw ConductorClientException("Unable to start workflow:${startWorkflowRequest.name}, version:$version", e)
        }
    }
}