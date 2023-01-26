package com.netflix.conductor.client.kotlin.http.jersey

import com.netflix.conductor.client.kotlin.config.ConductorClientConfiguration
import com.netflix.conductor.client.kotlin.config.DefaultConductorClientConfiguration
import com.netflix.conductor.client.kotlin.exception.ConductorClientException
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
class JerseyTaskClient : JerseyBaseClient {
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
    constructor(config: ClientConfig, handler: ClientHandler, vararg filters: ClientFilter) : this(
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
    ) : super(JerseyClientRequestHandler(config, handler, *filters), clientConfiguration)

    internal constructor(requestHandler: JerseyClientRequestHandler) : super(requestHandler, null)

    /**
     * Perform a poll for a task of a specific task type.
     *
     * @param taskType The taskType to poll for
     * @param domain The domain of the task type
     * @param workerId Name of the client worker. Used for logging.
     * @return Task waiting to be executed.
     */
    suspend fun pollTask(taskType: String, workerId: String, domain: String): Task {
        Validate.notBlank(taskType, "Task type cannot be blank")
        Validate.notBlank(workerId, "Worker id cannot be blank")
        val params = arrayOf<Any>("workerid", workerId, "domain", domain)
        val task = ObjectUtils.defaultIfNull<Task>(
            getForEntity(
                "tasks/poll/{taskType}", params,
                Task::class.java, taskType
            ),
            Task()
        )
        populateTaskPayloads(task)
        return task
    }

    /**
     * Perform a batch poll for tasks by task type. Batch size is configurable by count.
     *
     * @param taskType Type of task to poll for
     * @param workerId Name of the client worker. Used for logging.
     * @param count Maximum number of tasks to be returned. Actual number of tasks returned can be
     * less than this number.
     * @param timeoutInMillisecond Long poll wait timeout.
     * @return List of tasks awaiting to be executed.
     */
    suspend fun batchPollTasksByTaskType(
        taskType: String, workerId: String, count: Int, timeoutInMillisecond: Int
    ): List<Task>? {
        Validate.notBlank(taskType, "Task type cannot be blank")
        Validate.notBlank(workerId, "Worker id cannot be blank")
        Validate.isTrue(count > 0, "Count must be greater than 0")
        val params = arrayOf<Any>(
            "workerid", workerId, "count", count, "timeout", timeoutInMillisecond
        )
        val tasks: List<Task>? = getForEntity("tasks/poll/batch/{taskType}", params, taskList, taskType)
        tasks?.forEach(Consumer { task: Task ->
            populateTaskPayloads(
                task
            )
        })
        return tasks
    }

    /**
     * Batch poll for tasks in a domain. Batch size is configurable by count.
     *
     * @param taskType Type of task to poll for
     * @param domain The domain of the task type
     * @param workerId Name of the client worker. Used for logging.
     * @param count Maximum number of tasks to be returned. Actual number of tasks returned can be
     * less than this number.
     * @param timeoutInMillisecond Long poll wait timeout.
     * @return List of tasks awaiting to be executed.
     */
    suspend fun batchPollTasksInDomain(
        taskType: String, domain: String, workerId: String, count: Int, timeoutInMillisecond: Int
    ): List<Task>? {
        Validate.notBlank(taskType, "Task type cannot be blank")
        Validate.notBlank(workerId, "Worker id cannot be blank")
        Validate.isTrue(count > 0, "Count must be greater than 0")
        val params = arrayOf<Any>(
            "workerid",
            workerId,
            "count",
            count,
            "timeout",
            timeoutInMillisecond,
            "domain",
            domain
        )
        val tasks: List<Task>? = getForEntity("tasks/poll/batch/{taskType}", params, taskList, taskType)
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
            task.inputData = downloadFromExternalStorage(
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
            task.outputData = downloadFromExternalStorage(
                ExternalPayloadStorage.PayloadType.TASK_OUTPUT,
                task.externalOutputPayloadStoragePath
            )
            task.externalOutputPayloadStoragePath = null
        }
    }

    /**
     * Updates the result of a task execution. If the size of the task output payload is bigger than
     * [ConductorClientConfiguration.getTaskOutputPayloadThresholdKB], it is uploaded to
     * [ExternalPayloadStorage], if enabled, else the task is marked as
     * FAILED_WITH_TERMINAL_ERROR.
     *
     * @param taskResult the [TaskResult] of the executed task to be updated.
     */
    suspend fun updateTask(taskResult: TaskResult) {
        postForEntityWithRequestOnly("tasks", taskResult)
    }

    fun evaluateAndUploadLargePayload(
        taskOutputData: Map<String?, Any?>?, taskType: String?
    ): String? {
        try {
            ByteArrayOutputStream().use { byteArrayOutputStream ->
                objectMapper.writeValue(byteArrayOutputStream, taskOutputData)
                val taskOutputBytes = byteArrayOutputStream.toByteArray()
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
                    return uploadToExternalPayloadStorage(
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

    /**
     * Ack for the task poll.
     *
     * @param taskId Id of the task to be polled
     * @param workerId user identified worker.
     * @return true if the task was found with the given ID and acknowledged. False otherwise. If
     * the server returns false, the client should NOT attempt to ack again.
     */
    suspend fun ack(taskId: String, workerId: String): Boolean {
        Validate.notBlank(taskId, "Task id cannot be blank")
        val response: String? = postForEntity(
            "tasks/{taskId}/ack",
            null, arrayOf("workerid", workerId),
            String::class.java,
            taskId
        )
        return java.lang.Boolean.valueOf(response)
    }

    /**
     * Log execution messages for a task.
     *
     * @param taskId id of the task
     * @param logMessage the message to be logged
     */
    suspend fun logMessageForTask(taskId: String, logMessage: String?) {
        Validate.notBlank(taskId, "Task id cannot be blank")
        postForEntityWithRequestOnly("tasks/$taskId/log", logMessage)
    }

    /**
     * Fetch execution logs for a task.
     *
     * @param taskId id of the task.
     */
    suspend fun getTaskLogs(taskId: String): List<TaskExecLog>? {
        Validate.notBlank(taskId, "Task id cannot be blank")
        return getForEntity("tasks/{taskId}/log", null, taskExecLogList, taskId)
    }

    /**
     * Retrieve information about the task
     *
     * @param taskId ID of the task
     * @return Task details
     */
    suspend fun getTaskDetails(taskId: String): Task? {
        Validate.notBlank(taskId, "Task id cannot be blank")
        return getForEntity(
            "tasks/{taskId}", null,
            Task::class.java, taskId
        )
    }

    /**
     * Removes a task from a taskType queue
     *
     * @param taskType the taskType to identify the queue
     * @param taskId the id of the task to be removed
     */
    suspend fun removeTaskFromQueue(taskType: String, taskId: String) {
        Validate.notBlank(taskType, "Task type cannot be blank")
        Validate.notBlank(taskId, "Task id cannot be blank")
        delete(url = "tasks/queue/{taskType}/{taskId}", uriVariables = arrayOf(taskType, taskId))
    }

    suspend fun getQueueSizeForTask(taskType: String): Int {
        Validate.notBlank(taskType, "Task type cannot be blank")
        val queueSize: Int? = getForEntity(
            "tasks/queue/size", arrayOf("taskType", taskType),
            object : GenericType<Int?>() {})
        return queueSize ?: 0
    }

    suspend fun getQueueSizeForTask(
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
        val queueSize: Int? = getForEntity(
            "tasks/queue/size",
            params.toTypedArray(),
            object : GenericType<Int?>() {})
        return queueSize ?: 0
    }

    /**
     * Get last poll data for a given task type
     *
     * @param taskType the task type for which poll data is to be fetched
     * @return returns the list of poll data for the task type
     */
    suspend fun getPollData(taskType: String): List<PollData>? {
        Validate.notBlank(taskType, "Task type cannot be blank")
        val params = arrayOf<Any>("taskType", taskType)
        return getForEntity("tasks/queue/polldata", params, pollDataList)
    }

    /**
     * Get the last poll data for all task types
     *
     * @return returns a list of poll data for all task types
     */
    suspend fun getAllPollData(): List<PollData>? = getForEntity("tasks/queue/polldata/all", null, pollDataList)

    /**
     * Requeue pending tasks for all running workflows
     *
     * @return returns the number of tasks that have been requeued
     */
    suspend fun requeueAllPendingTasks(): String? = postForEntity("tasks/queue/requeue", null, null, String::class.java)

    /**
     * Requeue pending tasks of a specific task type
     *
     * @return returns the number of tasks that have been requeued
     */
    suspend fun requeuePendingTasksByTaskType(taskType: String): String? {
        Validate.notBlank(taskType, "Task type cannot be blank")
        return postForEntity(
            "tasks/queue/requeue/{taskType}", null, null,
            String::class.java, taskType
        )
    }

    /**
     * Search for tasks based on payload
     *
     * @param query the search string
     * @return returns the [SearchResult] containing the [TaskSummary] matching the
     * query
     */
    suspend fun search(query: String): SearchResult<TaskSummary>? = getForEntity(
        "tasks/search",
        arrayOf("query", query),
        searchResultTaskSummary
    )

    /**
     * Search for tasks based on payload
     *
     * @param query the search string
     * @return returns the [SearchResult] containing the [Task] matching the query
     */
    suspend fun searchV2(query: String): SearchResult<Task>? = getForEntity(
        "tasks/search-v2", arrayOf("query", query), searchResultTask
    )

    /**
     * Paginated search for tasks based on payload
     *
     * @param start start value of page
     * @param size number of tasks to be returned
     * @param sort sort order
     * @param freeText additional free text query
     * @param query the search query
     * @return the [SearchResult] containing the [TaskSummary] that match the query
     */
    suspend fun search(
        start: Int, size: Int, sort: String, freeText: String, query: String
    ): SearchResult<TaskSummary>? {
        val params = arrayOf<Any>(
            "start", start, "size", size, "sort", sort, "freeText", freeText, "query", query
        )
        return getForEntity("tasks/search", params, searchResultTaskSummary)
    }

    /**
     * Paginated search for tasks based on payload
     *
     * @param start start value of page
     * @param size number of tasks to be returned
     * @param sort sort order
     * @param freeText additional free text query
     * @param query the search query
     * @return the [SearchResult] containing the [Task] that match the query
     */
    suspend fun searchV2(
        start: Int, size: Int, sort: String, freeText: String, query: String
    ): SearchResult<Task>? {
        val params = arrayOf<Any>(
            "start", start, "size", size, "sort", sort, "freeText", freeText, "query", query
        )
        return getForEntity("tasks/search-v2", params, searchResultTask)
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