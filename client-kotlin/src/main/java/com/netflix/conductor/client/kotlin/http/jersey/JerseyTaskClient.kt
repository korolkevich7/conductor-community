package com.netflix.conductor.client.kotlin.http.jersey

import com.netflix.conductor.client.kotlin.config.ConductorClientConfiguration
import com.netflix.conductor.client.kotlin.config.DefaultConductorClientConfiguration
import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.client.kotlin.http.TaskClient
import com.netflix.conductor.client.kotlin.telemetry.MetricsContainer
import com.netflix.conductor.common.metadata.tasks.PollData
import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.conductor.common.metadata.tasks.TaskExecLog
import com.netflix.conductor.common.metadata.tasks.TaskResult
import com.netflix.conductor.common.run.SearchResult
import com.netflix.conductor.common.run.TaskSummary
import com.netflix.conductor.common.utils.ExternalPayloadStorage
import com.sun.jersey.api.client.ClientHandler
import com.sun.jersey.api.client.GenericType
import com.sun.jersey.api.client.config.ClientConfig
import com.sun.jersey.api.client.config.DefaultClientConfig
import com.sun.jersey.api.client.filter.ClientFilter
import org.apache.commons.lang3.ObjectUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.Validate
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.*
import java.util.function.Consumer

/** Client for conductor task management including polling for task, updating task status etc.  */
open class JerseyTaskClient : TaskClient {

    private var jerseyBaseClient: JerseyBaseClient

    /** Creates a default task client  */
    constructor() : this(
        DefaultClientConfig(),
        DefaultConductorClientConfiguration(),
        null
    )

    /**
     * @param config REST Client configuration
     */
    constructor(config: ClientConfig) : this(
        config,
        DefaultConductorClientConfiguration(),
        null
    )

    /**
     * @param config REST Client configuration
     * @param handler Jersey client handler. Useful when plugging in various http client interaction
     * modules (e.g. ribbon)
     */
    constructor(config: ClientConfig, handler: ClientHandler?) : this(
        config,
        DefaultConductorClientConfiguration(),
        handler
    )

    /**
     * @param config REST Client configuration
     * @param handler Jersey client handler. Useful when plugging in various http client interaction
     * modules (e.g. ribbon)
     * @param filters Chain of client side filters to be applied per request
     */
    constructor(config: ClientConfig, handler: ClientHandler?, vararg filters: ClientFilter) : this(
        config,
        DefaultConductorClientConfiguration(),
        handler,
        *filters
    )

    /**
     * @param config REST Client configuration
     * @param clientConfiguration Specific properties configured for the client, see [     ]
     * @param handler Jersey client handler. Useful when plugging in various http client interaction
     * modules (e.g. ribbon)
     * @param filters Chain of client side filters to be applied per request
     */
    constructor(
        config: ClientConfig,
        clientConfiguration: ConductorClientConfiguration?,
        handler: ClientHandler?,
        vararg filters: ClientFilter
    ) {
        jerseyBaseClient = JerseyBaseClient(JerseyClientRequestHandler(config, handler, *filters), clientConfiguration)
    }

    internal constructor(requestHandler: JerseyClientRequestHandler) {
        jerseyBaseClient = JerseyBaseClient(requestHandler, null)
    }

    override fun setRootURI(root: String) {
        jerseyBaseClient.setRootURI(root)
    }

    override suspend fun pollTask(taskType: String, workerId: String, domain: String): Task {
        Validate.notBlank(taskType, "Task type cannot be blank")
        Validate.notBlank(workerId, "Worker id cannot be blank")
        val params = arrayOf<Any?>("workerid", workerId, "domain", domain)
        val task = ObjectUtils.defaultIfNull<Task>(
            jerseyBaseClient.getForEntity(
                "tasks/poll/{taskType}", params,
                Task::class.java, taskType
            ),
            Task()
        )
        populateTaskPayloads(task)
        return task
    }

    override suspend fun batchPollTasksByTaskType(
        taskType: String, workerId: String, count: Int, timeoutInMillisecond: Int
    ): List<Task>? {
        Validate.notBlank(taskType, "Task type cannot be blank")
        Validate.notBlank(workerId, "Worker id cannot be blank")
        Validate.isTrue(count > 0, "Count must be greater than 0")
        val params = arrayOf<Any?>(
            "workerid", workerId, "count", count, "timeout", timeoutInMillisecond
        )
        val tasks: List<Task>? = jerseyBaseClient.getForEntity("tasks/poll/batch/{taskType}", params, taskList, taskType)
        tasks?.forEach(Consumer { task: Task ->
            populateTaskPayloads(
                task
            )
        })
        return tasks
    }

    override suspend fun batchPollTasksInDomain(
        taskType: String, domain: String?, workerId: String, count: Int, timeoutInMillisecond: Int
    ): List<Task>? {
        Validate.notBlank(taskType, "Task type cannot be blank")
        Validate.notBlank(workerId, "Worker id cannot be blank")
        Validate.isTrue(count > 0, "Count must be greater than 0")
        val params = arrayOf<Any?>(
            "workerid",
            workerId,
            "count",
            count,
            "timeout",
            timeoutInMillisecond,
            "domain",
            domain
        )
        val tasks: List<Task>? = jerseyBaseClient.getForEntity("tasks/poll/batch/{taskType}", params, taskList, taskType)
        tasks?.forEach(Consumer { task: Task ->
            populateTaskPayloads(
                task
            )
        })
        return tasks
    }

    /**
     * Populates the task input/output from external payload storage if the external storage path is
     * specified.
     *
     * @param task the task for which the input is to be populated.
     */
    private fun populateTaskPayloads(task: Task) {
        if (StringUtils.isNotBlank(task.externalInputPayloadStoragePath)) {
            MetricsContainer.incrementExternalPayloadUsedCount(
                task.taskDefName,
                ExternalPayloadStorage.Operation.READ.name,
                ExternalPayloadStorage.PayloadType.TASK_INPUT.name
            )
            task.inputData = jerseyBaseClient.downloadFromExternalStorage(
                ExternalPayloadStorage.PayloadType.TASK_INPUT,
                task.externalInputPayloadStoragePath
            )
            task.externalInputPayloadStoragePath = null
        }
        if (StringUtils.isNotBlank(task.externalOutputPayloadStoragePath)) {
            MetricsContainer.incrementExternalPayloadUsedCount(
                task.taskDefName,
                ExternalPayloadStorage.Operation.READ.name,
                ExternalPayloadStorage.PayloadType.TASK_OUTPUT.name
            )
            task.outputData = jerseyBaseClient.downloadFromExternalStorage(
                ExternalPayloadStorage.PayloadType.TASK_OUTPUT,
                task.externalOutputPayloadStoragePath
            )
            task.externalOutputPayloadStoragePath = null
        }
    }

    override suspend fun updateTask(taskResult: TaskResult) =
        jerseyBaseClient.postForEntityWithRequestOnly("tasks", taskResult)

    override fun evaluateAndUploadLargePayload(
        taskOutputData: Map<String?, Any?>?, taskType: String
    ): String? {
        try {
            ByteArrayOutputStream().use { byteArrayOutputStream ->
                jerseyBaseClient.objectMapper.writeValue(byteArrayOutputStream, taskOutputData)
                val taskOutputBytes = byteArrayOutputStream.toByteArray()
                val taskResultSize = taskOutputBytes.size.toLong()
                MetricsContainer.recordTaskResultPayloadSize(taskType, taskResultSize)
                val payloadSizeThreshold: Long =
                    jerseyBaseClient.conductorClientConfiguration.taskOutputPayloadThresholdKB * 1024L
                if (taskResultSize > payloadSizeThreshold) {
                    require(
                        !(!jerseyBaseClient.conductorClientConfiguration.isExternalPayloadStorageEnabled
                                || taskResultSize
                                > jerseyBaseClient.conductorClientConfiguration.taskOutputMaxPayloadThresholdKB
                                * 1024L)
                    ) {
                        "The TaskResult payload size: $taskResultSize is greater than the permissible $payloadSizeThreshold bytes"
                    }
                    MetricsContainer.incrementExternalPayloadUsedCount(
                        taskType,
                        ExternalPayloadStorage.Operation.WRITE.name,
                        ExternalPayloadStorage.PayloadType.TASK_OUTPUT.name
                    )
                    return jerseyBaseClient.uploadToExternalPayloadStorage(
                        ExternalPayloadStorage.PayloadType.TASK_OUTPUT, taskOutputBytes, taskResultSize
                    )
                }
                return null
            }
        } catch (e: IOException) {
            val errorMsg = "Unable to update task: $taskType with task result"
            LOGGER.error(errorMsg, e)
            throw ConductorClientException(errorMsg, e)
        }
    }

    override suspend fun ack(taskId: String, workerId: String): Boolean {
        Validate.notBlank(taskId, "Task id cannot be blank")
        val response: String? = jerseyBaseClient.postForEntity(
            "tasks/{taskId}/ack",
            null, arrayOf("workerid", workerId),
            String::class.java,
            taskId
        )
        return java.lang.Boolean.valueOf(response)
    }

    override suspend fun logMessageForTask(taskId: String, logMessage: String?) {
        Validate.notBlank(taskId, "Task id cannot be blank")
        jerseyBaseClient.postForEntityWithRequestOnly("tasks/$taskId/log", logMessage)
    }

    override suspend fun getTaskLogs(taskId: String): List<TaskExecLog>? {
        Validate.notBlank(taskId, "Task id cannot be blank")
        return jerseyBaseClient.getForEntity("tasks/{taskId}/log", null, taskExecLogList, taskId)
    }

    override suspend fun getTaskDetails(taskId: String): Task? {
        Validate.notBlank(taskId, "Task id cannot be blank")
        return jerseyBaseClient.getForEntity(
            "tasks/{taskId}", null,
            Task::class.java, taskId
        )
    }

    override suspend fun removeTaskFromQueue(taskType: String, taskId: String) {
        Validate.notBlank(taskType, "Task type cannot be blank")
        Validate.notBlank(taskId, "Task id cannot be blank")
        jerseyBaseClient.delete(url = "tasks/queue/{taskType}/{taskId}", uriVariables = arrayOf(taskType, taskId))
    }

    override suspend fun getQueueSizeForTask(taskType: String): Int {
        Validate.notBlank(taskType, "Task type cannot be blank")
        val queueSize: Int? = jerseyBaseClient.getForEntity(
            "tasks/queue/size", arrayOf("taskType", taskType),
            object : GenericType<Int?>() {})
        return queueSize ?: 0
    }

    override suspend fun getQueueSizeForTask(
        taskType: String, domain: String, isolationGroupId: String, executionNamespace: String
    ): Int {
        Validate.notBlank(taskType, "Task type cannot be blank")
        val params: MutableList<Any> = LinkedList()
        params.add("taskType")
        params.add(taskType)
        if (StringUtils.isNotBlank(domain)) {
            params.add("domain")
            params.add(domain)
        }
        if (StringUtils.isNotBlank(isolationGroupId)) {
            params.add("isolationGroupId")
            params.add(isolationGroupId)
        }
        if (StringUtils.isNotBlank(executionNamespace)) {
            params.add("executionNamespace")
            params.add(executionNamespace)
        }
        val queueSize: Int? = jerseyBaseClient.getForEntity(
            "tasks/queue/size",
            params.toTypedArray(),
            object : GenericType<Int?>() {})
        return queueSize ?: 0
    }

    override suspend fun getPollData(taskType: String): List<PollData>? {
        Validate.notBlank(taskType, "Task type cannot be blank")
        val params = arrayOf<Any?>("taskType", taskType)
        return jerseyBaseClient.getForEntity("tasks/queue/polldata", params, pollDataList)
    }

    override suspend fun getAllPollData(): List<PollData>? = jerseyBaseClient.getForEntity(
        "tasks/queue/polldata/all", null, pollDataList
    )

    override suspend fun requeueAllPendingTasks(): String? = jerseyBaseClient.postForEntity(
        "tasks/queue/requeue", null, null, String::class.java
    )

    override suspend fun requeuePendingTasksByTaskType(taskType: String): String? {
        Validate.notBlank(taskType, "Task type cannot be blank")
        return jerseyBaseClient.postForEntity(
            "tasks/queue/requeue/{taskType}", null, null,
            String::class.java, taskType
        )
    }

    override suspend fun search(query: String): SearchResult<TaskSummary>? = jerseyBaseClient.getForEntity(
        "tasks/search",
        arrayOf("query", query),
        searchResultTaskSummary
    )

    override suspend fun searchV2(query: String): SearchResult<Task>? = jerseyBaseClient.getForEntity(
        "tasks/search-v2", arrayOf("query", query), searchResultTask
    )

    override suspend fun search(
        start: Int, size: Int, sort: String, freeText: String, query: String
    ): SearchResult<TaskSummary>? {
        val params = arrayOf<Any?>(
            "start", start, "size", size, "sort", sort, "freeText", freeText, "query", query
        )
        return jerseyBaseClient.getForEntity("tasks/search", params, searchResultTaskSummary)
    }

    override suspend fun searchV2(
        start: Int, size: Int, sort: String, freeText: String, query: String
    ): SearchResult<Task>? {
        val params = arrayOf<Any?>(
            "start", start, "size", size, "sort", sort, "freeText", freeText, "query", query
        )
        return jerseyBaseClient.getForEntity("tasks/search-v2", params, searchResultTask)
    }

    companion object {
        internal val taskList: GenericType<List<Task>> = object : GenericType<List<Task>>() {}
        internal val taskExecLogList: GenericType<List<TaskExecLog>> = object : GenericType<List<TaskExecLog>>() {}
        internal val pollDataList: GenericType<List<PollData>> = object : GenericType<List<PollData>>() {}
        internal val searchResultTaskSummary: GenericType<SearchResult<TaskSummary>> =
            object : GenericType<SearchResult<TaskSummary>>() {}
        internal val searchResultTask: GenericType<SearchResult<Task>> = object : GenericType<SearchResult<Task>>() {}
        internal val queueSizeMap: GenericType<Map<String, Int>> = object : GenericType<Map<String, Int>>() {}
        private val LOGGER = LoggerFactory.getLogger(JerseyTaskClient::class.java)
    }
}