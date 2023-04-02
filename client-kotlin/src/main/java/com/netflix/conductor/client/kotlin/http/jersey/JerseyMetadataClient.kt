package com.netflix.conductor.client.kotlin.http.jersey

import com.netflix.conductor.client.kotlin.config.ConductorClientConfiguration
import com.netflix.conductor.client.kotlin.config.DefaultConductorClientConfiguration
import com.netflix.conductor.client.kotlin.http.MetadataClient
import com.netflix.conductor.common.metadata.tasks.TaskDef
import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.sun.jersey.api.client.ClientHandler
import com.sun.jersey.api.client.config.ClientConfig
import com.sun.jersey.api.client.config.DefaultClientConfig
import com.sun.jersey.api.client.filter.ClientFilter
import org.apache.commons.lang3.Validate

open class JerseyMetadataClient : MetadataClient {

    private var jerseyBaseClient: JerseyBaseClient

    /** Creates a default metadata client  */
    constructor() : this(
        DefaultClientConfig(),
        DefaultConductorClientConfiguration(),
        null
    )

    /**
     * @param config config REST Client configuration
     * @param handler handler Jersey client handler. Useful when plugging in various http client
     * interaction modules (e.g. ribbon)
     * @param filters Chain of client side filters to be applied per request
     */
    constructor(config: ClientConfig, handler: ClientHandler? = null, vararg filters: ClientFilter) : this(
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

    // Workflow Metadata Operations
    override suspend fun registerWorkflowDef(workflowDef: WorkflowDef) = jerseyBaseClient.postForEntityWithRequestOnly(
        "metadata/workflow", workflowDef
    )

    override suspend fun validateWorkflowDef(workflowDef: WorkflowDef) = jerseyBaseClient.postForEntityWithRequestOnly(
        "metadata/workflow/validate", workflowDef
    )

    override suspend fun updateWorkflowDefs(workflowDefs: List<WorkflowDef>) = jerseyBaseClient.put(
        "metadata/workflow", null, workflowDefs
    )

    override suspend fun getWorkflowDef(name: String, version: Int?): WorkflowDef? {
        Validate.notBlank(name, "name cannot be blank")
        return jerseyBaseClient.getForEntity(
            "metadata/workflow/{name}", arrayOf("version", version),
            WorkflowDef::class.java,
            name
        )
    }

    override suspend fun unregisterWorkflowDef(name: String, version: Int) {
        Validate.notBlank(name, "Workflow name cannot be blank")
        jerseyBaseClient.delete(url = "metadata/workflow/{name}/{version}", uriVariables = arrayOf(name, version))
    }

    // Task Metadata Operations
    override suspend fun registerTaskDefs(taskDefs: List<TaskDef>) = jerseyBaseClient.postForEntityWithRequestOnly(
        "metadata/taskdefs", taskDefs
    )

    override suspend fun updateTaskDef(taskDef: TaskDef) = jerseyBaseClient.put("metadata/taskdefs", null, taskDef)

    override suspend fun getTaskDef(taskType: String): TaskDef? {
        Validate.notBlank(taskType, "Task type cannot be blank")
        return jerseyBaseClient.getForEntity(
            "metadata/taskdefs/{tasktype}", null,
            TaskDef::class.java, taskType
        )
    }

    override suspend fun unregisterTaskDef(taskType: String) {
        Validate.notBlank(taskType, "Task type cannot be blank")
        jerseyBaseClient.delete(url = "metadata/taskdefs/{tasktype}", uriVariables = arrayOf(taskType))
    }
}