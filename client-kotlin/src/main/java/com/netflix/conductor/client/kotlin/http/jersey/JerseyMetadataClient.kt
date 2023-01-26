package com.netflix.conductor.client.kotlin.http.jersey

import com.netflix.conductor.client.kotlin.config.ConductorClientConfiguration
import com.netflix.conductor.client.kotlin.config.DefaultConductorClientConfiguration
import com.netflix.conductor.common.metadata.tasks.TaskDef
import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.sun.jersey.api.client.ClientHandler
import com.sun.jersey.api.client.config.ClientConfig
import com.sun.jersey.api.client.config.DefaultClientConfig
import com.sun.jersey.api.client.filter.ClientFilter
import org.apache.commons.lang3.Validate

class JerseyMetadataClient : JerseyBaseClient {
    /** Creates a default metadata client  */
    constructor() : this(
        DefaultClientConfig(),
        DefaultConductorClientConfiguration(),
        null
    )

    /**
     * @param clientConfig REST Client configuration
     */
    constructor(clientConfig: ClientConfig) : this(
        clientConfig,
        DefaultConductorClientConfiguration(),
        null
    )

    /**
     * @param clientConfig REST Client configuration
     * @param clientHandler Jersey client handler. Useful when plugging in various http client
     * interaction modules (e.g. ribbon)
     */
    constructor(clientConfig: ClientConfig, clientHandler: ClientHandler) : this(
        clientConfig,
        DefaultConductorClientConfiguration(),
        clientHandler
    )

    /**
     * @param config config REST Client configuration
     * @param handler handler Jersey client handler. Useful when plugging in various http client
     * interaction modules (e.g. ribbon)
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
        clientConfiguration: ConductorClientConfiguration,
        handler: ClientHandler?,
        vararg filters: ClientFilter
    ) : super(JerseyClientRequestHandler(config, handler, *filters), clientConfiguration)

    internal constructor(requestHandler: JerseyClientRequestHandler) : super(requestHandler, null)

    // Workflow Metadata Operations
    /**
     * Register a workflow definition with the server
     *
     * @param workflowDef the workflow definition
     */
    suspend fun registerWorkflowDef(workflowDef: WorkflowDef) = postForEntityWithRequestOnly(
        "metadata/workflow", workflowDef
    )

    suspend fun validateWorkflowDef(workflowDef: WorkflowDef) = postForEntityWithRequestOnly(
        "metadata/workflow/validate", workflowDef
    )

    /**
     * Updates a list of existing workflow definitions
     *
     * @param workflowDefs List of workflow definitions to be updated
     */
    suspend fun updateWorkflowDefs(workflowDefs: List<WorkflowDef>) = put(
        "metadata/workflow", null, workflowDefs
    )

    /**
     * Retrieve the workflow definition
     *
     * @param name the name of the workflow
     * @param version the version of the workflow def
     * @return Workflow definition for the given workflow and version
     */
    suspend fun getWorkflowDef(name: String, version: Int): WorkflowDef? {
        Validate.notBlank(name, "name cannot be blank")
        return getForEntity(
            "metadata/workflow/{name}", arrayOf("version", version),
            WorkflowDef::class.java,
            name
        )
    }

    /**
     * Removes the workflow definition of a workflow from the conductor server. It does not remove
     * associated workflows. Use with caution.
     *
     * @param name Name of the workflow to be unregistered.
     * @param version Version of the workflow definition to be unregistered.
     */
    suspend fun unregisterWorkflowDef(name: String, version: Int) {
        Validate.notBlank(name, "Workflow name cannot be blank")
        delete(url = "metadata/workflow/{name}/{version}", uriVariables = arrayOf(name, version))
    }

    // Task Metadata Operations
    /**
     * Registers a list of task types with the conductor server
     *
     * @param taskDefs List of task types to be registered.
     */
    suspend fun registerTaskDefs(taskDefs: List<TaskDef>) = postForEntityWithRequestOnly(
        "metadata/taskdefs", taskDefs
    )

    /**
     * Updates an existing task definition
     *
     * @param taskDef the task definition to be updated
     */
    suspend fun updateTaskDef(taskDef: TaskDef) = put("metadata/taskdefs", null, taskDef)

    /**
     * Retrieve the task definition of a given task type
     *
     * @param taskType type of task for which to retrieve the definition
     * @return Task Definition for the given task type
     */
    suspend fun getTaskDef(taskType: String): TaskDef? {
        Validate.notBlank(taskType, "Task type cannot be blank")
        return getForEntity(
            "metadata/taskdefs/{tasktype}", null,
            TaskDef::class.java, taskType
        )
    }

    /**
     * Removes the task definition of a task type from the conductor server. Use with caution.
     *
     * @param taskType Task type to be unregistered.
     */
    suspend fun unregisterTaskDef(taskType: String) {
        Validate.notBlank(taskType, "Task type cannot be blank")
        delete(url = "metadata/taskdefs/{tasktype}", uriVariables = arrayOf(taskType))
    }
}