package com.netflix.conductor.client.kotlin.http.ktor

import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.client.kotlin.http.PayloadStorage
import com.netflix.conductor.client.kotlin.http.TaskClient
import com.netflix.conductor.client.kotlin.telemetry.*
import com.netflix.conductor.common.metadata.tasks.PollData
import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.conductor.common.metadata.tasks.TaskExecLog
import com.netflix.conductor.common.metadata.tasks.TaskResult
import com.netflix.conductor.common.run.SearchResult
import com.netflix.conductor.common.run.TaskSummary
import com.netflix.conductor.common.utils.ExternalPayloadStorage
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.request.*
import io.ktor.utils.io.errors.IOException

class KtorTaskClient(rootURI: String, httpClient: HttpClient): TaskClient, KtorBaseClient(rootURI, httpClient) {
    companion object {
        const val TASK_ID_NOT_BLANK = "Task id cannot be blank"
        const val TASK_TYPE_NOT_BLANK = "Task type cannot be blank"
        const val WORKER_ID_NOT_BLANK = "Worker id cannot be blank"
        const val COUNT_CONDITION = "Count must be greater than 0"
    }
    override suspend fun pollTask(taskType: String, workerId: String, domain: String): Task {
        require(taskType.isNotBlank()) { TASK_TYPE_NOT_BLANK }
        require(workerId.isNotBlank()) { WORKER_ID_NOT_BLANK }
        val response = httpClient.get {
            url("$rootURI/tasks/poll/$taskType")
            parameter("workerid", workerId)
            parameter("domain", domain)
        }
        val task: Task = response.body()
        populateTaskPayloads(task)
        return task
    }

    override suspend fun batchPollTasksByTaskType(
        taskType: String,
        workerId: String,
        count: Int,
        timeoutInMillisecond: Int
    ): List<Task> = batchPollTasksInDomain(taskType, null, workerId, count, timeoutInMillisecond)

    override suspend fun batchPollTasksInDomain(
        taskType: String,
        domain: String?,
        workerId: String,
        count: Int,
        timeoutInMillisecond: Int
    ): List<Task> {
        require(taskType.isNotBlank()) { TASK_TYPE_NOT_BLANK }
        require(workerId.isNotBlank()) { WORKER_ID_NOT_BLANK }
        require(count > 0) { COUNT_CONDITION }
        val response = httpClient.get {
            url("$rootURI/tasks/poll/$taskType")
            parameter("workerid", workerId)
            parameter("count", count)
            parameter("timeout", timeoutInMillisecond)
            parameter("domain", domain)
        }
        val tasks: List<Task> = response.body()
        tasks.forEach { populateTaskPayloads(it) }
        return tasks
    }

    override suspend fun updateTask(taskResult: TaskResult) {
        httpClient.post {
            url("$rootURI/tasks")
            setBody(taskResult)
        }
    }

    override suspend fun evaluateAndUploadLargePayload(taskOutputData: Map<String, Any?>, taskType: String): String? {
        try {
            val taskOutputBytes = objectMapper.writeValueAsBytes(taskOutputData)
            val taskResultSize = taskOutputBytes.size.toLong()
            MetricsContainer.recordTaskResultPayloadSize(taskType, taskResultSize)
            val payloadSizeThreshold: Long =
                conductorClientConfiguration.taskOutputPayloadThresholdKB * 1024L
            if (taskResultSize > payloadSizeThreshold) {
                require(
                    !(!conductorClientConfiguration.isExternalPayloadStorageEnabled
                            || taskResultSize
                            > conductorClientConfiguration.taskOutputMaxPayloadThresholdKB
                            * 1024L)
                ) {
                    "The TaskResult payload size: $taskResultSize is greater than the permissible $payloadSizeThreshold bytes"
                }
                MetricsContainer.incrementExternalPayloadUsedCount(
                    taskType,
                    ExternalPayloadStorage.Operation.WRITE.name,
                    ExternalPayloadStorage.PayloadType.TASK_OUTPUT.name
                )
                return uploadToPayloadStorage(
                    PayloadStorage.PayloadType.TASK_OUTPUT, taskOutputBytes, taskResultSize
                )
            }
            return null
        } catch (e: IOException) {
            throw ConductorClientException("Unable to update task: $taskType with task result", e)
        }
    }

    override suspend fun ack(taskId: String, workerId: String): Boolean {
        require(taskId.isNotBlank()) { "Task id cannot be blank" }
        val response = httpClient.post {
            url("$rootURI/tasks/$taskId/ack")
            parameter("workerid", workerId)
        }
        return response.body()
    }

    override suspend fun logMessageForTask(taskId: String, logMessage: String) {
        require(taskId.isNotBlank()) { "Task id cannot be blank" }
        httpClient.post {
            url("$rootURI/tasks/$taskId/log")
            setBody(logMessage)
        }
    }

    override suspend fun getTaskLogs(taskId: String): List<TaskExecLog> {
        require(taskId.isNotBlank()) { TASK_ID_NOT_BLANK }
        val response = httpClient.get {
            url("$rootURI/tasks/$taskId/log")
        }
        return response.body()
    }

    override suspend fun getTaskDetails(taskId: String): Task {
        require(taskId.isNotBlank()) { TASK_ID_NOT_BLANK }
        val response = httpClient.get {
            url("$rootURI/tasks/$taskId")
        }
        return response.body()
    }

    override suspend fun removeTaskFromQueue(taskType: String, taskId: String) {
        require(taskType.isNotBlank()) { TASK_TYPE_NOT_BLANK }
        require(taskId.isNotBlank()) { TASK_ID_NOT_BLANK }
        httpClient.delete {
            url("$rootURI/tasks/queue/$taskType/$taskId")
        }
    }

    override suspend fun getQueueSizeForTask(taskType: String): Int {
        require(taskType.isNotBlank()) { TASK_TYPE_NOT_BLANK }
        val response = httpClient.get {
            url("$rootURI/tasks/queue/size")
            parameter("taskType", taskType)
        }
        return response.body()
    }

    override suspend fun getQueueSizeForTask(
        taskType: String,
        domain: String,
        isolationGroupId: String,
        executionNamespace: String
    ): Int {
        require(taskType.isNotBlank()) { TASK_TYPE_NOT_BLANK }
        val response = httpClient.get {
            url("$rootURI/tasks/queue/size")
            parameter("taskType", taskType)
            if (domain.isNotBlank()) parameter("domain", domain)
            if (isolationGroupId.isNotBlank()) parameter("isolationGroupId", isolationGroupId)
            if (executionNamespace.isNotBlank()) parameter("executionNamespace", executionNamespace)
        }
        return response.body()
    }

    override suspend fun getPollData(taskType: String): List<PollData> {
        require(taskType.isNotBlank()) { TASK_TYPE_NOT_BLANK }
        val response = httpClient.get {
            url("$rootURI/tasks/queue/polldata")
            parameter("taskType", taskType)
        }
        return response.body()
    }

    override suspend fun getAllPollData(): List<PollData> =
        httpClient.get("$rootURI/tasks/queue/polldata/all").body()

    override suspend fun requeueAllPendingTasks(): String =
        httpClient.post("$rootURI/tasks/queue/requeue").body()

    override suspend fun requeuePendingTasksByTaskType(taskType: String): String? {
        require(taskType.isNotBlank()) { TASK_TYPE_NOT_BLANK }
        val response = httpClient.post {
            url("$rootURI/tasks/queue/requeue/$taskType")
        }
        return response.body()
    }

    override suspend fun search(query: String): SearchResult<TaskSummary> {
        val response = httpClient.post {
            url("$rootURI/tasks/search")
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
    ): SearchResult<TaskSummary> {
        val response = httpClient.post {
            url("$rootURI/tasks/search")
            parameter("start", start)
            parameter("size", size)
            parameter("sort", sort)
            parameter("freeText", freeText)
            parameter("query", query)
        }
        return response.body()
    }

    override suspend fun searchV2(query: String): SearchResult<Task> {
        val response = httpClient.post {
            url("$rootURI/tasks/search-v2")
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
    ): SearchResult<Task> {
        val response = httpClient.post {
            url("$rootURI/tasks/search-v2")
            parameter("start", start)
            parameter("size", size)
            parameter("sort", sort)
            parameter("freeText", freeText)
            parameter("query", query)
        }
        return response.body()
    }

    private suspend fun populateTaskPayloads(task: Task) {
        if (task.externalInputPayloadStoragePath?.isNotBlank() == true) {
            MetricsContainer.incrementExternalPayloadUsedCount(
                task.taskDefName,
                PayloadStorage.Operation.READ.name,
                PayloadStorage.PayloadType.TASK_INPUT.name
            )
            task.inputData = downloadFromExternalStorage(
                PayloadStorage.PayloadType.TASK_INPUT,
                task.externalInputPayloadStoragePath
            )
            task.externalInputPayloadStoragePath = null
        }
        if (task.externalOutputPayloadStoragePath?.isNotBlank() == true) {
            MetricsContainer.incrementExternalPayloadUsedCount(
                task.taskDefName,
                PayloadStorage.Operation.READ.name,
                PayloadStorage.PayloadType.TASK_OUTPUT.name
            )
            task.outputData = downloadFromExternalStorage(
                PayloadStorage.PayloadType.TASK_OUTPUT,
                task.externalOutputPayloadStoragePath
            )
            task.externalOutputPayloadStoragePath = null
        }
    }
}