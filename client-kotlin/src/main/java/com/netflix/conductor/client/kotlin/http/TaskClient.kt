package com.netflix.conductor.client.kotlin.http

import com.netflix.conductor.client.kotlin.config.ConductorClientConfiguration
import com.netflix.conductor.common.metadata.tasks.PollData
import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.conductor.common.metadata.tasks.TaskExecLog
import com.netflix.conductor.common.metadata.tasks.TaskResult
import com.netflix.conductor.common.run.SearchResult
import com.netflix.conductor.common.run.TaskSummary
import com.netflix.conductor.common.utils.ExternalPayloadStorage

/** Client for conductor task management including polling for task, updating task status etc.  */
interface TaskClient {
    val rootURI: String

    /**
     * Perform a poll for a task of a specific task type.
     *
     * @param taskType The taskType to poll for
     * @param domain The domain of the task type
     * @param workerId Name of the client worker. Used for logging.
     * @return Task waiting to be executed.
     */
    suspend fun pollTask(taskType: String, workerId: String, domain: String): Task

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
    ): List<Task>

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
        taskType: String, domain: String?, workerId: String, count: Int, timeoutInMillisecond: Int
    ): List<Task>

    /**
     * Updates the result of a task execution. If the size of the task output payload is bigger than
     * [ConductorClientConfiguration.getTaskOutputPayloadThresholdKB], it is uploaded to
     * [ExternalPayloadStorage], if enabled, else the task is marked as
     * FAILED_WITH_TERMINAL_ERROR.
     *
     * @param taskResult the [TaskResult] of the executed task to be updated.
     */
    suspend fun updateTask(taskResult: TaskResult)

    suspend fun evaluateAndUploadLargePayload(
        taskOutputData: Map<String, Any?>, taskType: String
    ): String?

    /**
     * Ack for the task poll.
     *
     * @param taskId Id of the task to be polled
     * @param workerId user identified worker.
     * @return true if the task was found with the given ID and acknowledged. False otherwise. If
     * the server returns false, the client should NOT attempt to ack again.
     */
    suspend fun ack(taskId: String, workerId: String): Boolean

    /**
     * Log execution messages for a task.
     *
     * @param taskId id of the task
     * @param logMessage the message to be logged
     */
    suspend fun logMessageForTask(taskId: String, logMessage: String)


    /**
     * Fetch execution logs for a task.
     *
     * @param taskId id of the task.
     */
    suspend fun getTaskLogs(taskId: String): List<TaskExecLog>

    /**
     * Retrieve information about the task
     *
     * @param taskId ID of the task
     * @return Task details
     */
    suspend fun getTaskDetails(taskId: String): Task

    /**
     * Removes a task from a taskType queue
     *
     * @param taskType the taskType to identify the queue
     * @param taskId the id of the task to be removed
     */
    suspend fun removeTaskFromQueue(taskType: String, taskId: String)

    suspend fun getQueueSizeForTask(taskType: String): Int

    suspend fun getQueueSizeForTask(
        taskType: String, domain: String, isolationGroupId: String, executionNamespace: String
    ): Int

    /**
     * Get last poll data for a given task type
     *
     * @param taskType the task type for which poll data is to be fetched
     * @return returns the list of poll data for the task type
     */
    suspend fun getPollData(taskType: String): List<PollData>

    /**
     * Get the last poll data for all task types
     *
     * @return returns a list of poll data for all task types
     */
    suspend fun getAllPollData(): List<PollData>

    /**
     * Requeue pending tasks for all running workflows
     *
     * @return returns the number of tasks that have been requeued
     */
    suspend fun requeueAllPendingTasks(): String

    /**
     * Requeue pending tasks of a specific task type
     *
     * @return returns the number of tasks that have been requeued
     */
    suspend fun requeuePendingTasksByTaskType(taskType: String): String?

    /**
     * Search for tasks based on payload
     *
     * @param query the search string
     * @return returns the [SearchResult] containing the [TaskSummary] matching the
     * query
     */
    suspend fun search(query: String): SearchResult<TaskSummary>

    /**
     * Search for tasks based on payload
     *
     * @param query the search string
     * @return returns the [SearchResult] containing the [Task] matching the query
     */
    suspend fun searchV2(query: String): SearchResult<Task>

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
    ): SearchResult<TaskSummary>

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
    ): SearchResult<Task>
}