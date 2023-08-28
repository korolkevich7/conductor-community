package com.netflix.conductor.client.kotlin.http

import com.netflix.conductor.common.metadata.tasks.TaskDef
import com.netflix.conductor.common.metadata.workflow.WorkflowDef

interface MetadataClient {

    var rootURI: String

    /**
     * Register a workflow definition with the server
     *
     * @param workflowDef the workflow definition
     */
    suspend fun registerWorkflowDef(workflowDef: WorkflowDef)

    suspend fun validateWorkflowDef(workflowDef: WorkflowDef)

    /**
     * Updates a list of existing workflow definitions
     *
     * @param workflowDefs List of workflow definitions to be updated
     */
    suspend fun updateWorkflowDefs(workflowDefs: List<WorkflowDef>)

    /**
     * Retrieve the workflow definition
     *
     * @param name the name of the workflow
     * @param version the version of the workflow def
     * @return Workflow definition for the given workflow and version
     */
    suspend fun getWorkflowDef(name: String, version: Int?): WorkflowDef

    /**
     * Removes the workflow definition of a workflow from the conductor server. It does not remove
     * associated workflows. Use with caution.
     *
     * @param name Name of the workflow to be unregistered.
     * @param version Version of the workflow definition to be unregistered.
     */
    suspend fun unregisterWorkflowDef(name: String, version: Int)

    /**
     * Registers a list of task types with the conductor server
     *
     * @param taskDefs List of task types to be registered.
     */
    suspend fun registerTaskDefs(taskDefs: List<TaskDef>)

    /**
     * Updates an existing task definition
     *
     * @param taskDef the task definition to be updated
     */
    suspend fun updateTaskDef(taskDef: TaskDef)

    /**
     * Retrieve the task definition of a given task type
     *
     * @param taskType type of task for which to retrieve the definition
     * @return Task Definition for the given task type
     */
    suspend fun getTaskDef(taskType: String): TaskDef?

    /**
     * Removes the task definition of a task type from the conductor server. Use with caution.
     *
     * @param taskType Task type to be unregistered.
     */
    suspend fun unregisterTaskDef(taskType: String)
}