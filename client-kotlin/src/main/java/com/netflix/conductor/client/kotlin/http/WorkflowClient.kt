package com.netflix.conductor.client.kotlin.http

import com.netflix.conductor.client.kotlin.config.ConductorClientConfiguration
import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.common.metadata.workflow.RerunWorkflowRequest
import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest
import com.netflix.conductor.common.model.BulkResponse
import com.netflix.conductor.common.run.SearchResult
import com.netflix.conductor.common.run.Workflow
import com.netflix.conductor.common.run.WorkflowSummary
import com.netflix.conductor.common.utils.ExternalPayloadStorage

interface WorkflowClient {

    val rootURI: String

    /**
     * Starts a workflow. If the size of the workflow input payload is bigger than [ ][ConductorClientConfiguration.getWorkflowInputPayloadThresholdKB], it is uploaded to [ ], if enabled, else the workflow is rejected.
     *
     * @param startWorkflowRequest the [StartWorkflowRequest] object to start the workflow.
     * @return the id of the workflow instance that can be used for tracking.
     * @throws ConductorClientException if [ExternalPayloadStorage] is disabled or if the
     * payload size is greater than [     ][ConductorClientConfiguration.getWorkflowInputMaxPayloadThresholdKB].
     * @throws NullPointerException if [StartWorkflowRequest] is null or [     ][StartWorkflowRequest.getName] is null.
     * @throws IllegalArgumentException if [StartWorkflowRequest.getName] is empty.
     */
    suspend fun startWorkflow(startWorkflowRequest: StartWorkflowRequest): String

    /**
     * Retrieve a workflow by workflow id
     *
     * @param workflowId the id of the workflow
     * @param includeTasks specify if the tasks in the workflow need to be returned
     * @return the requested workflow
     */
    suspend fun getWorkflow(workflowId: String, includeTasks: Boolean): Workflow

    /**
     * Retrieve all workflows for a given correlation id and name
     *
     * @param name the name of the workflow
     * @param correlationId the correlation id
     * @param includeClosed specify if all workflows are to be returned or only running workflows
     * @param includeTasks specify if the tasks in the workflow need to be returned
     * @return list of workflows for the given correlation id and name
     */
    suspend fun getWorkflows(
        name: String, correlationId: String, includeClosed: Boolean, includeTasks: Boolean
    ): List<Workflow>

    /**
     * Removes a workflow from the system
     *
     * @param workflowId the id of the workflow to be deleted
     * @param archiveWorkflow flag to indicate if the workflow should be archived before deletion
     */
    suspend fun deleteWorkflow(workflowId: String, archiveWorkflow: Boolean)

    /**
     * Terminates the execution of all given workflows instances
     *
     * @param workflowIds the ids of the workflows to be terminated
     * @param reason the reason to be logged and displayed
     * @return the [BulkResponse] contains bulkErrorResults and bulkSuccessfulResults
     */
    suspend fun terminateWorkflows(workflowIds: List<String>, reason: String): BulkResponse?

    /**
     * Retrieve all running workflow instances for a given name and version
     *
     * @param workflowName the name of the workflow
     * @param version the version of the wokflow definition. Defaults to 1.
     * @return the list of running workflow instances
     */
    suspend fun getRunningWorkflow(workflowName: String, version: Int): List<String>

    /**
     * Retrieve all workflow instances for a given workflow name between a specific time period
     *
     * @param workflowName the name of the workflow
     * @param version the version of the workflow definition. Defaults to 1.
     * @param startTime the start time of the period
     * @param endTime the end time of the period
     * @return returns a list of workflows created during the specified during the time period
     */
    suspend fun getWorkflowsByTimePeriod(
        workflowName: String, version: Int, startTime: Long, endTime: Long
    ): List<String>

    /**
     * Starts the decision task for the given workflow instance
     *
     * @param workflowId the id of the workflow instance
     */
    suspend fun runDecider(workflowId: String)

    /**
     * Pause a workflow by workflow id
     *
     * @param workflowId the workflow id of the workflow to be paused
     */
    suspend fun pauseWorkflow(workflowId: String)


    /**
     * Resume a paused workflow by workflow id
     *
     * @param workflowId the workflow id of the paused workflow
     */
    suspend fun resumeWorkflow(workflowId: String)

    /**
     * Skips a given task from a current RUNNING workflow
     *
     * @param workflowId the id of the workflow instance
     * @param taskReferenceName the reference name of the task to be skipped
     */
    suspend fun skipTaskFromWorkflow(workflowId: String, taskReferenceName: String)

    /**
     * Reruns the workflow from a specific task
     *
     * @param workflowId the id of the workflow
     * @param rerunWorkflowRequest the request containing the task to rerun from
     * @return the id of the workflow
     */
    suspend fun rerunWorkflow(workflowId: String, rerunWorkflowRequest: RerunWorkflowRequest): String

    /**
     * Restart a completed workflow
     *
     * @param workflowId the workflow id of the workflow to be restarted
     * @param useLatestDefinitions if true, use the latest workflow and task definitions when
     * restarting the workflow if false, use the workflow and task definitions embedded in the
     * workflow execution when restarting the workflow
     */
    suspend fun restart(workflowId: String, useLatestDefinitions: Boolean)

    /**
     * Retries the last failed task in a workflow
     *
     * @param workflowId the workflow id of the workflow with the failed task
     */
    suspend fun retryLastFailedTask(workflowId: String)

    /**
     * Resets the callback times of all IN PROGRESS tasks to 0 for the given workflow
     *
     * @param workflowId the id of the workflow
     */
    suspend fun resetCallbacksForInProgressTasks(workflowId: String)

    /**
     * Terminates the execution of the given workflow instance
     *
     * @param workflowId the id of the workflow to be terminated
     * @param reason the reason to be logged and displayed
     */
    suspend fun terminateWorkflow(workflowId: String, reason: String)

    /**
     * Search for workflows based on payload
     *
     * @param query the search query
     * @return the [SearchResult] containing the [WorkflowSummary] that match the query
     */
    suspend fun search(query: String): SearchResult<WorkflowSummary>

    /**
     * Search for workflows based on payload
     *
     * @param query the search query
     * @return the [SearchResult] containing the [Workflow] that match the query
     */
    suspend fun searchV2(query: String): SearchResult<Workflow>

    /**
     * Paginated search for workflows based on payload
     *
     * @param start start value of page
     * @param size number of workflows to be returned
     * @param sort sort order
     * @param freeText additional free text query
     * @param query the search query
     * @return the [SearchResult] containing the [WorkflowSummary] that match the query
     */
    suspend fun search(
        start: Int, size: Int, sort: String, freeText: String, query: String
    ): SearchResult<WorkflowSummary>

    /**
     * Paginated search for workflows based on payload
     *
     * @param start start value of page
     * @param size number of workflows to be returned
     * @param sort sort order
     * @param freeText additional free text query
     * @param query the search query
     * @return the [SearchResult] containing the [Workflow] that match the query
     */
    suspend fun searchV2(
        start: Int, size: Int, sort: String, freeText: String, query: String
    ): SearchResult<Workflow>
}