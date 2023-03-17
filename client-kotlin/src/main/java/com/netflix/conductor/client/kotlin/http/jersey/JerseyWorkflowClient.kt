package com.netflix.conductor.client.kotlin.http.jersey

import com.netflix.conductor.client.kotlin.config.ConductorClientConfiguration
import com.netflix.conductor.client.kotlin.config.DefaultConductorClientConfiguration
import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.client.kotlin.http.WorkflowClient
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


open class JerseyWorkflowClient : WorkflowClient {

    private var jerseyBaseClient: JerseyBaseClient

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
    ) {
        jerseyBaseClient = JerseyBaseClient(JerseyClientRequestHandler(config, handler, *filters), clientConfiguration)
    }

    internal constructor(requestHandler: JerseyClientRequestHandler) {
        jerseyBaseClient = JerseyBaseClient(requestHandler, null)
    }

    override fun setRootURI(root: String) {
        jerseyBaseClient.setRootURI(root)
    }

    override suspend fun startWorkflow(startWorkflowRequest: StartWorkflowRequest): String? {
        Validate.notBlank(startWorkflowRequest.name, "Workflow name cannot be null or empty")
        Validate.isTrue(
            StringUtils.isBlank(startWorkflowRequest.externalInputPayloadStoragePath),
            "External Storage Path must not be set"
        )
        val version = if (startWorkflowRequest.version != null) startWorkflowRequest.version.toString() else "latest"
        try {
            ByteArrayOutputStream().use { byteArrayOutputStream ->
                jerseyBaseClient.objectMapper.writeValue(byteArrayOutputStream, startWorkflowRequest.input)
                val workflowInputBytes = byteArrayOutputStream.toByteArray()
                val workflowInputSize = workflowInputBytes.size.toLong()
                MetricsContainer.recordWorkflowInputPayloadSize(
                    startWorkflowRequest.name, version, workflowInputSize
                )
                if (workflowInputSize
                    > jerseyBaseClient.conductorClientConfiguration.workflowInputPayloadThresholdKB * 1024L
                ) {
                    if (!jerseyBaseClient.conductorClientConfiguration.isExternalPayloadStorageEnabled
                        || (workflowInputSize
                                > jerseyBaseClient.conductorClientConfiguration.workflowInputMaxPayloadThresholdKB
                                * 1024L)
                    ) {
                        val errorMsg = "Input payload larger than the allowed threshold of:" +
                                " ${jerseyBaseClient.conductorClientConfiguration.workflowInputPayloadThresholdKB} KB"
                        throw ConductorClientException(errorMsg)
                    } else {
                        MetricsContainer.incrementExternalPayloadUsedCount(
                            startWorkflowRequest.name,
                            ExternalPayloadStorage.Operation.WRITE.name,
                            ExternalPayloadStorage.PayloadType.WORKFLOW_INPUT.name
                        )
                        val externalStoragePath: String = jerseyBaseClient.uploadToExternalPayloadStorage(
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
            jerseyBaseClient.postForEntity(
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

    override suspend fun getWorkflow(workflowId: String, includeTasks: Boolean): Workflow? {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        val workflow: Workflow? = jerseyBaseClient.getForEntity(
            "workflow/{workflowId}", arrayOf("includeTasks", includeTasks),
            Workflow::class.java,
            workflowId
        )
        populateWorkflowOutput(workflow)
        return workflow
    }

    override suspend fun getWorkflows(
        name: String, correlationId: String, includeClosed: Boolean, includeTasks: Boolean
    ): List<Workflow?>? {
        Validate.notBlank(name, "name cannot be blank")
        Validate.notBlank(correlationId, "correlationId cannot be blank")
        val params = arrayOf<Any?>("includeClosed", includeClosed, "includeTasks", includeTasks)
        val workflows: List<Workflow?>? = jerseyBaseClient.getForEntity(
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
                it.output = jerseyBaseClient.downloadFromExternalStorage(
                    ExternalPayloadStorage.PayloadType.WORKFLOW_OUTPUT,
                    it.externalOutputPayloadStoragePath
                )
            }
        }
    }

    override suspend fun deleteWorkflow(workflowId: String, archiveWorkflow: Boolean) {
        Validate.notBlank(workflowId, "Workflow id cannot be blank")
        val params = arrayOf<Any?>("archiveWorkflow", archiveWorkflow)
        jerseyBaseClient.delete(queryParams = params, url = "workflow/{workflowId}/remove", uriVariables = arrayOf(workflowId))
    }

    override suspend fun terminateWorkflows(workflowIds: List<String?>, reason: String): BulkResponse? {
        Validate.isTrue(workflowIds.isNotEmpty(), "workflow id cannot be blank")
        return jerseyBaseClient.postForEntity(
            "workflow/bulk/terminate",
            workflowIds, arrayOf("reason", reason),
            BulkResponse::class.java
        )
    }

    override suspend fun getRunningWorkflow(workflowName: String, version: Int): List<String?>? {
        Validate.notBlank(workflowName, "Workflow name cannot be blank")
        return jerseyBaseClient.getForEntity(
            "workflow/running/{name}", arrayOf("version", version),
            object : GenericType<List<String?>?>() {},
            workflowName
        )
    }

    override suspend fun getWorkflowsByTimePeriod(
        workflowName: String, version: Int, startTime: Long, endTime: Long
    ): List<String?>? {
        Validate.notBlank(workflowName, "Workflow name cannot be blank")
        val params = arrayOf<Any?>("version", version, "startTime", startTime, "endTime", endTime)
        return jerseyBaseClient.getForEntity(
            "workflow/running/{name}",
            params,
            object : GenericType<List<String?>?>() {},
            workflowName
        )
    }

    override suspend fun runDecider(workflowId: String) {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        jerseyBaseClient.put("workflow/decide/{workflowId}", null, null, workflowId)
    }

    override suspend fun pauseWorkflow(workflowId: String) {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        jerseyBaseClient.put("workflow/{workflowId}/pause", null, null, workflowId)
    }

    override suspend fun resumeWorkflow(workflowId: String) {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        jerseyBaseClient.put("workflow/{workflowId}/resume", null, null, workflowId)
    }

    override suspend fun skipTaskFromWorkflow(workflowId: String, taskReferenceName: String) {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        Validate.notBlank(taskReferenceName, "Task reference name cannot be blank")
        jerseyBaseClient.put(
            "workflow/{workflowId}/skiptask/{taskReferenceName}",
            null,
            null,
            workflowId,
            taskReferenceName
        )
    }

    override suspend fun rerunWorkflow(workflowId: String, rerunWorkflowRequest: RerunWorkflowRequest): String? {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        return jerseyBaseClient.postForEntity(
            "workflow/{workflowId}/rerun",
            rerunWorkflowRequest,
            null,
            String::class.java,
            workflowId
        )
    }

    override suspend fun restart(workflowId: String, useLatestDefinitions: Boolean) {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        val params = arrayOf<Any?>("useLatestDefinitions", useLatestDefinitions)
        jerseyBaseClient.postForEntity<Void>("workflow/{workflowId}/restart", null, params, Void.TYPE, workflowId)
    }

    override suspend fun retryLastFailedTask(workflowId: String) {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        jerseyBaseClient.postForEntityWithUriVariablesOnly("workflow/{workflowId}/retry", workflowId)
    }

    override suspend fun resetCallbacksForInProgressTasks(workflowId: String) {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        jerseyBaseClient.postForEntityWithUriVariablesOnly("workflow/{workflowId}/resetcallbacks", workflowId)
    }

    override suspend fun terminateWorkflow(workflowId: String, reason: String) {
        Validate.notBlank(workflowId, "workflow id cannot be blank")
        jerseyBaseClient.delete(arrayOf("reason", reason), "workflow/{workflowId}", workflowId)
    }

    override suspend fun search(query: String): SearchResult<WorkflowSummary>? = jerseyBaseClient.getForEntity(
        "workflow/search",
        arrayOf("query", query),
        searchResultWorkflowSummary
    )

    override suspend fun searchV2(query: String): SearchResult<Workflow>? = jerseyBaseClient.getForEntity(
        "workflow/search-v2",
        arrayOf("query", query),
        searchResultWorkflow
    )

    override suspend fun search(
        start: Int, size: Int, sort: String, freeText: String, query: String
    ): SearchResult<WorkflowSummary>? {
        val params = arrayOf<Any?>(
            "start", start, "size", size, "sort", sort, "freeText", freeText, "query", query
        )
        return jerseyBaseClient.getForEntity(
            "workflow/search",
            params,
            searchResultWorkflowSummary
        )
    }

    override suspend fun searchV2(
        start: Int, size: Int, sort: String, freeText: String, query: String
    ): SearchResult<Workflow>? {
        val params = arrayOf<Any?>(
            "start", start, "size", size, "sort", sort, "freeText", freeText, "query", query
        )
        return jerseyBaseClient.getForEntity(
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