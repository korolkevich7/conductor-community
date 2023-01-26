package com.netflix.conductor.client.kotlin.http.jersey

import com.netflix.conductor.client.kotlin.config.ConductorClientConfiguration
import com.netflix.conductor.client.kotlin.config.DefaultConductorClientConfiguration
import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.client.kotlin.telemetry.MetricsContainer
import com.netflix.conductor.common.metadata.workflow.RerunWorkflowRequest
import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest
import com.netflix.conductor.common.model.BulkResponse
import com.netflix.conductor.common.run.SearchResult
import com.netflix.conductor.common.run.Workflow
import com.netflix.conductor.common.run.WorkflowSummary
import com.netflix.conductor.common.utils.ExternalPayloadStorage
import com.sun.jersey.api.client.ClientHandler
import com.sun.jersey.api.client.GenericType
import com.sun.jersey.api.client.config.ClientConfig
import com.sun.jersey.api.client.config.DefaultClientConfig
import com.sun.jersey.api.client.filter.ClientFilter
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.Validate
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.function.Consumer


class JerseyWorkflowClient : JerseyBaseClient {
    /** Creates a default workflow client  */
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
        clientConfiguration: ConductorClientConfiguration,
        handler: ClientHandler?,
        vararg filters: ClientFilter
    ) : super(JerseyClientRequestHandler(config, handler, *filters), clientConfiguration)

    internal constructor(requestHandler: JerseyClientRequestHandler) : super(requestHandler, null)

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
    suspend fun startWorkflow(startWorkflowRequest: StartWorkflowRequest): String? {
        Validate.notBlank(startWorkflowRequest.name, "Workflow name cannot be null or empty")
        Validate.isTrue(
            StringUtils.isBlank(startWorkflowRequest.externalInputPayloadStoragePath),
            "External Storage Path must not be set"
        )
        val version = if (startWorkflowRequest.version != null) startWorkflowRequest.version.toString() else "latest"
        try {
            ByteArrayOutputStream().use { byteArrayOutputStream ->
                objectMapper.writeValue(byteArrayOutputStream, startWorkflowRequest.input)
                val workflowInputBytes = byteArrayOutputStream.toByteArray()
                val workflowInputSize = workflowInputBytes.size.toLong()
                MetricsContainer.recordWorkflowInputPayloadSize(
                    startWorkflowRequest.name, version, workflowInputSize
                )
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
                    } else {
                        MetricsContainer.incrementExternalPayloadUsedCount(
                            startWorkflowRequest.name,
                            ExternalPayloadStorage.Operation.WRITE.name,
                            ExternalPayloadStorage.PayloadType.WORKFLOW_INPUT.name
                        )
                        val externalStoragePath: String = uploadToExternalPayloadStorage(
                            ExternalPayloadStorage.PayloadType.WORKFLOW_INPUT,
                            workflowInputBytes,
                            workflowInputSize
                        )
                        startWorkflowRequest.externalInputPayloadStoragePath = externalStoragePath
                        startWorkflowRequest.input = null
                    }
                }
            }
        } catch (e: IOException) {
            val errorMsg = String.format(
                "Unable to start workflow:%s, version:%s",
                startWorkflowRequest.name, version
            )
            LOGGER.error(errorMsg, e)
            MetricsContainer.incrementWorkflowStartErrorCount(
                startWorkflowRequest.name,
                e
            )
            throw ConductorClientException(errorMsg, e)
        }
        return try {
            postForEntity(
                "workflow",
                startWorkflowRequest,
                null,
                String::class.java,
                startWorkflowRequest.name
            )
        } catch (e: ConductorClientException) {
            val errorMsg = String.format(
                "Unable to send start workflow request:%s, version:%s",
                startWorkflowRequest.name, version
            )
            LOGGER.error(errorMsg, e)
            MetricsContainer.incrementWorkflowStartErrorCount(
                startWorkflowRequest.name,
                e
            )
            throw e
        }
    }

    /**
     * Retrieve a workflow by workflow id
     *
     * @param workflowId the id of the workflow
     * @param includeTasks specify if the tasks in the workflow need to be returned
     * @return the requested workflow
     */
    suspend fun getWorkflow(workflowId: String, includeTasks: Boolean): Workflow? {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        val workflow: Workflow? = getForEntity(
            "workflow/{workflowId}", arrayOf("includeTasks", includeTasks),
            Workflow::class.java,
            workflowId
        )
        populateWorkflowOutput(workflow)
        return workflow
    }

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
    ): List<Workflow?>? {
        Validate.notBlank(name, "name cannot be blank")
        Validate.notBlank(correlationId, "correlationId cannot be blank")
        val params = arrayOf<Any>("includeClosed", includeClosed, "includeTasks", includeTasks)
        val workflows: List<Workflow?>? = getForEntity(
            url = "workflow/{name}/correlated/{correlationId}",
            queryParams = params,
            responseType = object : GenericType<List<Workflow?>?>() {},
            uriVariables = arrayOf(name, correlationId)
        )
        workflows?.forEach(Consumer { workflow: Workflow? ->
            populateWorkflowOutput(
                workflow
            )
        })
        return workflows
    }

    /**
     * Populates the workflow output from external payload storage if the external storage path is
     * specified.
     *
     * @param workflow the workflow for which the output is to be populated.
     */
    private fun populateWorkflowOutput(workflow: Workflow?) {
        workflow?.let {
            if (StringUtils.isNotBlank(it.externalOutputPayloadStoragePath)) {
                MetricsContainer.incrementExternalPayloadUsedCount(
                    it.workflowName,
                    ExternalPayloadStorage.Operation.READ.name,
                    ExternalPayloadStorage.PayloadType.WORKFLOW_OUTPUT.name
                )
                it.output = downloadFromExternalStorage(
                    ExternalPayloadStorage.PayloadType.WORKFLOW_OUTPUT,
                    it.externalOutputPayloadStoragePath
                )
            }
        }
    }

    /**
     * Removes a workflow from the system
     *
     * @param workflowId the id of the workflow to be deleted
     * @param archiveWorkflow flag to indicate if the workflow should be archived before deletion
     */
    suspend fun deleteWorkflow(workflowId: String, archiveWorkflow: Boolean) {
        Validate.notBlank(workflowId, "Workflow id cannot be blank")
        val params = arrayOf<Any>("archiveWorkflow", archiveWorkflow)
        delete(queryParams = params, url = "workflow/{workflowId}/remove", uriVariables = arrayOf(workflowId))
    }

    /**
     * Terminates the execution of all given workflows instances
     *
     * @param workflowIds the ids of the workflows to be terminated
     * @param reason the reason to be logged and displayed
     * @return the [BulkResponse] contains bulkErrorResults and bulkSuccessfulResults
     */
    suspend fun terminateWorkflows(workflowIds: List<String?>, reason: String): BulkResponse? {
        Validate.isTrue(workflowIds.isNotEmpty(), "workflow id cannot be blank")
        return postForEntity(
            "workflow/bulk/terminate",
            workflowIds, arrayOf("reason", reason),
            BulkResponse::class.java
        )
    }

    /**
     * Retrieve all running workflow instances for a given name and version
     *
     * @param workflowName the name of the workflow
     * @param version the version of the wokflow definition. Defaults to 1.
     * @return the list of running workflow instances
     */
    suspend fun getRunningWorkflow(workflowName: String, version: Int): List<String?>? {
        Validate.notBlank(workflowName, "Workflow name cannot be blank")
        return getForEntity(
            "workflow/running/{name}", arrayOf("version", version),
            object : GenericType<List<String?>?>() {},
            workflowName
        )
    }

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
    ): List<String?>? {
        Validate.notBlank(workflowName, "Workflow name cannot be blank")
        val params = arrayOf<Any>("version", version, "startTime", startTime, "endTime", endTime)
        return getForEntity(
            "workflow/running/{name}",
            params,
            object : GenericType<List<String?>?>() {},
            workflowName
        )
    }

    /**
     * Starts the decision task for the given workflow instance
     *
     * @param workflowId the id of the workflow instance
     */
    suspend fun runDecider(workflowId: String) {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        put("workflow/decide/{workflowId}", null, null, workflowId)
    }

    /**
     * Pause a workflow by workflow id
     *
     * @param workflowId the workflow id of the workflow to be paused
     */
    suspend fun pauseWorkflow(workflowId: String) {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        put("workflow/{workflowId}/pause", null, null, workflowId)
    }

    /**
     * Resume a paused workflow by workflow id
     *
     * @param workflowId the workflow id of the paused workflow
     */
    suspend fun resumeWorkflow(workflowId: String) {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        put("workflow/{workflowId}/resume", null, null, workflowId)
    }

    /**
     * Skips a given task from a current RUNNING workflow
     *
     * @param workflowId the id of the workflow instance
     * @param taskReferenceName the reference name of the task to be skipped
     */
    suspend fun skipTaskFromWorkflow(workflowId: String, taskReferenceName: String) {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        Validate.notBlank(taskReferenceName, "Task reference name cannot be blank")
        put(
            "workflow/{workflowId}/skiptask/{taskReferenceName}",
            null,
            null,
            workflowId,
            taskReferenceName
        )
    }

    /**
     * Reruns the workflow from a specific task
     *
     * @param workflowId the id of the workflow
     * @param rerunWorkflowRequest the request containing the task to rerun from
     * @return the id of the workflow
     */
    suspend fun rerunWorkflow(workflowId: String, rerunWorkflowRequest: RerunWorkflowRequest): String? {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        return postForEntity(
            "workflow/{workflowId}/rerun",
            rerunWorkflowRequest,
            null,
            String::class.java,
            workflowId
        )
    }

    /**
     * Restart a completed workflow
     *
     * @param workflowId the workflow id of the workflow to be restarted
     * @param useLatestDefinitions if true, use the latest workflow and task definitions when
     * restarting the workflow if false, use the workflow and task definitions embedded in the
     * workflow execution when restarting the workflow
     */
    suspend fun restart(workflowId: String, useLatestDefinitions: Boolean) {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        val params = arrayOf<Any>("useLatestDefinitions", useLatestDefinitions)
        postForEntity<Void>("workflow/{workflowId}/restart", null, params, Void.TYPE, workflowId)
    }

    /**
     * Retries the last failed task in a workflow
     *
     * @param workflowId the workflow id of the workflow with the failed task
     */
    suspend fun retryLastFailedTask(workflowId: String) {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        postForEntityWithUriVariablesOnly("workflow/{workflowId}/retry", workflowId)
    }

    /**
     * Resets the callback times of all IN PROGRESS tasks to 0 for the given workflow
     *
     * @param workflowId the id of the workflow
     */
    suspend fun resetCallbacksForInProgressTasks(workflowId: String) {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        postForEntityWithUriVariablesOnly("workflow/{workflowId}/resetcallbacks", workflowId)
    }

    /**
     * Terminates the execution of the given workflow instance
     *
     * @param workflowId the id of the workflow to be terminated
     * @param reason the reason to be logged and displayed
     */
    suspend fun terminateWorkflow(workflowId: String, reason: String) {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        delete(arrayOf("reason", reason), "workflow/{workflowId}", workflowId)
    }

    /**
     * Search for workflows based on payload
     *
     * @param query the search query
     * @return the [SearchResult] containing the [WorkflowSummary] that match the query
     */
    suspend fun search(query: String): SearchResult<WorkflowSummary>? = getForEntity(
        "workflow/search",
        arrayOf("query", query),
        searchResultWorkflowSummary
    )

    /**
     * Search for workflows based on payload
     *
     * @param query the search query
     * @return the [SearchResult] containing the [Workflow] that match the query
     */
    suspend fun searchV2(query: String): SearchResult<Workflow>? = getForEntity(
        "workflow/search-v2",
        arrayOf("query", query),
        searchResultWorkflow
    )

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
    ): SearchResult<WorkflowSummary>? {
        val params = arrayOf<Any>(
            "start", start, "size", size, "sort", sort, "freeText", freeText, "query", query
        )
        return getForEntity(
            "workflow/search",
            params,
            searchResultWorkflowSummary
        )
    }

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
    ): SearchResult<Workflow>? {
        val params = arrayOf<Any>(
            "start", start, "size", size, "sort", sort, "freeText", freeText, "query", query
        )
        return getForEntity(
            "workflow/search-v2",
            params,
            searchResultWorkflow
        )
    }

    companion object {
        internal val searchResultWorkflowSummary: GenericType<SearchResult<WorkflowSummary>> =
            object : GenericType<SearchResult<WorkflowSummary>>() {}
        internal val searchResultWorkflow: GenericType<SearchResult<Workflow>> =
            object : GenericType<SearchResult<Workflow>>() {}
        private val LOGGER: Logger =
            LoggerFactory.getLogger(JerseyWorkflowClient::class.java)
    }
}