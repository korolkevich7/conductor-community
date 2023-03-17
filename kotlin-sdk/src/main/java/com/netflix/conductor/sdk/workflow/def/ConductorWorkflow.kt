package com.netflix.conductor.sdk.workflow.def

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.type.TypeReference
import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.common.metadata.tasks.TaskDef
import com.netflix.conductor.common.metadata.tasks.TaskType
import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.netflix.conductor.common.metadata.workflow.WorkflowTask
import com.netflix.conductor.common.run.Workflow
import com.netflix.conductor.sdk.workflow.def.tasks.Task
import com.netflix.conductor.sdk.workflow.def.tasks.TaskRegistry
import com.netflix.conductor.sdk.workflow.executor.WorkflowExecutor
import com.netflix.conductor.sdk.workflow.utils.InputOutputGetter
import com.netflix.conductor.sdk.workflow.utils.ObjectMapperProvider
import kotlinx.coroutines.*
import java.util.*
import java.util.concurrent.CompletableFuture
import kotlin.coroutines.CoroutineContext

/**
 * @param <T> Type of the workflow input
</T> */
class ConductorWorkflow<T>(workflowExecutor: WorkflowExecutor?) : CoroutineScope {
    var name: String? = null
    var description: String? = null
    var version = 0
    var failureWorkflow: String? = null
    var ownerEmail: String? = null
    var timeoutPolicy: WorkflowDef.TimeoutPolicy? = null
    var workflowOutput: Map<String, Any> = HashMap()
    var timeoutSeconds: Long = 0
    var isRestartable = true
    var defaultInput: T? = null
        private set
    private var variables: Map<String, Any>? = null
    private val tasks: MutableList<Task<*>> = ArrayList()
    private val objectMapper = ObjectMapperProvider().objectMapper
    private val workflowExecutor: WorkflowExecutor?

    private val job = Job()
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + job

    init {
        this.workflowExecutor = workflowExecutor
        isRestartable = true
    }

    fun add(task: Task<*>) {
        tasks.add(task)
    }

    fun setDefaultInput(defaultInput: T) {
        this.defaultInput = defaultInput
    }

    fun getVariables(): Any? {
        return variables
    }

    fun setVariables(variables: Map<String, Any>?) {
        this.variables = variables
    }

    /**
     * Execute a dynamic workflow without creating a definition in metadata store.
     *
     *
     * <br></br>
     * **Note**: Use this with caution - as this does not promote re-usability of the workflows
     *
     * @param input Workflow Input - The input object is converted a JSON doc as an input to the
     * workflow
     * @return
     */
    fun executeDynamic(input: T): CompletableFuture<Workflow> {
        return workflowExecutor!!.executeWorkflow(this, input)
    }

    /**
     * Executes the workflow using registered metadata definitions
     *
     * @see .registerWorkflow
     * @param input
     * @return
     */
    fun execute(input: T): CompletableFuture<Workflow> {
        return workflowExecutor!!.executeWorkflow(name, version, input)
    }
    /**
     * @param overwrite set to true if the workflow should be overwritten if the definition already
     * exists with the given name and version. <font color=red>Use with caution</font>
     * @param registerTasks if set to true, missing task definitions are registered with the default
     * configuration.
     * @return true if success, false otherwise.
     */
    /**
     * Registers a new workflow in the server.
     *
     * @return true if the workflow is successfully registered. False if the workflow cannot be
     * registered and the workflow definition already exists on the server with given name +
     * version The call will throw a runtime exception if any of the tasks are missing
     * definitions on the server.
     */
    /**
     * @param overwrite set to true if the workflow should be overwritten if the definition already
     * exists with the given name and version. <font color=red>Use with caution</font>
     * @return true if success, false otherwise.
     */
    @JvmOverloads
    fun registerWorkflow(overwrite: Boolean = false, registerTasks: Boolean = false): Boolean {
        val workflowDef = toWorkflowDef()
        val missing = getMissingTasks(workflowDef)
        if (missing.isNotEmpty()) {
            if (!registerTasks) {
                throw RuntimeException(
                    "Workflow cannot be registered.  The following tasks do not have definitions.  "
                            + "Please register these tasks before creating the workflow.  Missing Tasks = "
                            + missing
                )
            } else {
                val ownerEmail = ownerEmail
                missing.stream().forEach { taskName: String ->
                    registerTaskDef(
                        taskName,
                        ownerEmail
                    )
                }
            }
        }
        return workflowExecutor!!.registerWorkflow(workflowDef, overwrite)
    }

    /**
     * @return Convert to the WorkflowDef model used by the Metadata APIs
     */
    fun toWorkflowDef(): WorkflowDef {
        val def = WorkflowDef()
        def.name = name
        def.description = description
        def.version = version
        def.failureWorkflow = failureWorkflow
        def.ownerEmail = ownerEmail
        def.timeoutPolicy = timeoutPolicy
        def.timeoutSeconds = timeoutSeconds
        def.isRestartable = isRestartable
        def.outputParameters = workflowOutput
        def.variables = variables
        def.inputTemplate = objectMapper.convertValue(defaultInput, object : TypeReference<MutableMap<String, Any>>() {})
        for (task: Task<*> in tasks) {
            def.tasks.addAll(task.workflowDefTasks)
        }
        return def
    }

    fun from(workflowName: String, workflowVersion: Int?): ConductorWorkflow<T> {
        val def = runBlocking { workflowExecutor!!.metadataClient.getWorkflowDef(workflowName, workflowVersion) }
        def?.let { fromWorkflowDef(this, it) }
        return this
    }

    private fun getMissingTasks(workflowDef: WorkflowDef): List<String> {
        val missing: MutableList<String> = ArrayList()
        workflowDef.collectTasks().stream()
            .filter { workflowTask: WorkflowTask -> (workflowTask.type == TaskType.TASK_TYPE_SIMPLE) }
            .map { obj: WorkflowTask -> obj.name }
            .distinct()
            .parallel()
            .forEach { taskName: String ->
                try {
                    val taskDef = runBlocking { workflowExecutor?.metadataClient?.getTaskDef(taskName) }
                } catch (cce: ConductorClientException) {
                    if (cce.status == 404) {
                        missing.add(taskName)
                    } else {
                        throw cce
                    }
                }
            }
        return missing
    }

    private fun registerTaskDef(taskName: String, ownerEmail: String?) {
        val taskDef = TaskDef()
        taskDef.name = taskName
        taskDef.ownerEmail = ownerEmail
        runBlocking { workflowExecutor!!.metadataClient.registerTaskDefs(listOf(taskDef)) }
    }

    override fun equals(o: Any?): Boolean {
        if (this === o) return true
        if (o == null || javaClass != o.javaClass) return false
        val workflow = o as ConductorWorkflow<*>
        return version == workflow.version && (name == workflow.name)
    }

    override fun hashCode(): Int {
        return Objects.hash(name, version)
    }

    override fun toString(): String {
        try {
            return objectMapper.writeValueAsString(toWorkflowDef())
        } catch (e: JsonProcessingException) {
            throw RuntimeException(e)
        }
    }

    companion object {
        val input = InputOutputGetter("workflow", InputOutputGetter.Field.input)
        val output = InputOutputGetter("workflow", InputOutputGetter.Field.output)

        /**
         * Generate ConductorWorkflow based on the workflow metadata definition
         *
         * @param def
         * @return
         */
        fun <T> fromWorkflowDef(def: WorkflowDef): ConductorWorkflow<T> {
            val workflow: ConductorWorkflow<T> = ConductorWorkflow(null)
            fromWorkflowDef(workflow, def)
            return workflow
        }

        private fun <T> fromWorkflowDef(workflow: ConductorWorkflow<T>, def: WorkflowDef) {
            workflow.name = def.name
            workflow.version = def.version
            workflow.failureWorkflow = def.failureWorkflow
            workflow.isRestartable = def.isRestartable
            workflow.setVariables(def.variables)
            workflow.setDefaultInput(def.inputTemplate as T)
            workflow.workflowOutput = def.outputParameters
            workflow.ownerEmail = def.ownerEmail
            workflow.description = def.description
            workflow.timeoutSeconds = def.timeoutSeconds
            workflow.timeoutPolicy = def.timeoutPolicy
            val workflowTasks = def.tasks
            for (workflowTask: WorkflowTask in workflowTasks) {
                val task = TaskRegistry.getTask(workflowTask)
                workflow.tasks.add(task)
            }
        }
    }
}