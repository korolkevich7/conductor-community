package com.netflix.conductor.sdk.workflow.executor

import com.fasterxml.jackson.core.type.TypeReference
import com.netflix.conductor.client.kotlin.http.MetadataClient
import com.netflix.conductor.client.kotlin.http.TaskClient
import com.netflix.conductor.client.kotlin.http.WorkflowClient
import com.netflix.conductor.client.kotlin.http.jersey.JerseyMetadataClient
import com.netflix.conductor.client.kotlin.http.jersey.JerseyTaskClient
import com.netflix.conductor.client.kotlin.http.jersey.JerseyWorkflowClient
import com.netflix.conductor.common.metadata.tasks.TaskDef
import com.netflix.conductor.common.metadata.tasks.TaskType
import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest
import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.netflix.conductor.common.run.Workflow
import com.netflix.conductor.sdk.workflow.def.ConductorWorkflow
import com.netflix.conductor.sdk.workflow.def.tasks.*
import com.netflix.conductor.sdk.workflow.executor.task.AnnotatedWorkerExecutor
import com.netflix.conductor.sdk.workflow.utils.ObjectMapperProvider
import com.sun.jersey.api.client.ClientHandler
import com.sun.jersey.api.client.config.DefaultClientConfig
import com.sun.jersey.api.client.filter.ClientFilter
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.*
import java.util.concurrent.*
import kotlin.coroutines.CoroutineContext

class WorkflowExecutor : CoroutineScope {
    private val listOfTaskDefs: TypeReference<List<TaskDef>> = object : TypeReference<List<TaskDef>>() {}
    private val runningWorkflowFutures: MutableMap<String, CompletableFuture<Workflow>> = ConcurrentHashMap()
    private val objectMapper = ObjectMapperProvider().objectMapper
    val taskClient: TaskClient
    private val workflowClient: WorkflowClient
    val metadataClient: MetadataClient
    private val annotatedWorkerExecutor: AnnotatedWorkerExecutor
    private val scheduledWorkflowMonitor = Executors.newSingleThreadScheduledExecutor()

    private val job = Job()
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + job

    constructor(apiServerURL: String) : this(apiServerURL, 100)

    constructor(
        apiServerURL: String, pollingInterval: Int, vararg clientFilter: ClientFilter
    ) {
        taskClient =
            JerseyTaskClient(DefaultClientConfig(), null as ClientHandler?, *clientFilter)
        taskClient.setRootURI(apiServerURL)
        workflowClient = JerseyWorkflowClient(
            DefaultClientConfig(),
            null as ClientHandler?,
            *clientFilter
        )
        workflowClient.setRootURI(apiServerURL)
        metadataClient = JerseyMetadataClient(
            DefaultClientConfig(),
            null as ClientHandler?,
            *clientFilter
        )
        metadataClient.setRootURI(apiServerURL)
        annotatedWorkerExecutor = AnnotatedWorkerExecutor(taskClient, pollingInterval)
        scheduledWorkflowMonitor.scheduleAtFixedRate(
            {
                for ((workflowId, future) in runningWorkflowFutures) {

                    val workflow = runBlocking {
                        workflowClient.getWorkflow(workflowId, true)
                    }

                    workflow?.let {
                        if (it.status.isTerminal) {
                            future.complete(it)
                        }
                    }
                }
            },
            100,
            100,
            TimeUnit.MILLISECONDS
        )
    }

    constructor(
        taskClient: TaskClient,
        workflowClient: WorkflowClient,
        metadataClient: MetadataClient,
        pollingInterval: Int
    ) {
        this.taskClient = taskClient
        this.workflowClient = workflowClient
        this.metadataClient = metadataClient
        annotatedWorkerExecutor = AnnotatedWorkerExecutor(taskClient, pollingInterval)
        scheduledWorkflowMonitor.scheduleAtFixedRate(
            {
                for ((workflowId, future) in runningWorkflowFutures) {

                        val workflow = runBlocking {
                            workflowClient.getWorkflow(workflowId, true)
                        }

                        workflow?.let {
                            if (it.status.isTerminal) {
                                future.complete(it)
                            }
                        }
                }
            },
            100,
            100,
            TimeUnit.MILLISECONDS
        )
    }

    fun initWorkers(packagesToScan: String?) {
        annotatedWorkerExecutor.initWorkers(packagesToScan)
    }

    fun executeWorkflow(name: String?, version: Int?, input: Any?): CompletableFuture<Workflow> {
        val future = CompletableFuture<Workflow>()
        val inputMap: Map<String, Any> = objectMapper.convertValue(
            input,
            object : TypeReference<MutableMap<String, Any>>() {}
        )
        val request = StartWorkflowRequest()
        request.input = inputMap
        request.name = name
        request.version = version

        val workflowId = runBlocking { workflowClient.startWorkflow(request) }

        workflowId?.let {
            runningWorkflowFutures[it] = future
        }
        return future
    }

    fun executeWorkflow(
        conductorWorkflow: ConductorWorkflow<*>, input: Any?
    ): CompletableFuture<Workflow> {
        val future = CompletableFuture<Workflow>()
        val inputMap: Map<String, Any> = objectMapper.convertValue(
            input,
            object : TypeReference<MutableMap<String, Any>>() {}
        )
        val request = StartWorkflowRequest()
        request.input = inputMap
        request.name = conductorWorkflow.name
        request.version = conductorWorkflow.version
        request.workflowDef = conductorWorkflow.toWorkflowDef()

        val workflowId = runBlocking { workflowClient.startWorkflow(request) }

        workflowId?.let {
            runningWorkflowFutures[it] = future
        }
        return future
    }

    @Throws(IOException::class)
    fun loadTaskDefs(resourcePath: String) {
        val resource = WorkflowExecutor::class.java.getResourceAsStream(resourcePath)
        if (resource != null) {
            val taskDefs = objectMapper.readValue(resource, listOfTaskDefs)
            loadMetadata(taskDefs)
        }
    }

    @Throws(IOException::class)
    fun loadWorkflowDefs(resourcePath: String) {
        val resource = WorkflowExecutor::class.java.getResourceAsStream(resourcePath)
        if (resource != null) {
            val workflowDef = objectMapper.readValue(
                resource,
                WorkflowDef::class.java
            )
            loadMetadata(workflowDef)
        }
    }

    fun loadMetadata(workflowDef: WorkflowDef) {
        runBlocking { metadataClient.registerWorkflowDef(workflowDef) }
    }

    fun loadMetadata(taskDefs: List<TaskDef>) {
        runBlocking { metadataClient.registerTaskDefs(taskDefs) }
    }

    fun shutdown() {
        scheduledWorkflowMonitor.shutdown()
        annotatedWorkerExecutor.shutdown()
    }

    fun registerWorkflow(workflowDef: WorkflowDef, overwrite: Boolean): Boolean {
        return try {
            runBlocking {
                if (overwrite) {
                    metadataClient.updateWorkflowDefs(listOf(workflowDef))
                } else {
                    metadataClient.registerWorkflowDef(workflowDef)
                }
            }
            true
        } catch (e: Exception) {
            LOGGER.error(e.message, e)
            false
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(WorkflowExecutor::class.java)

        init {
            initTaskImplementations()
        }

        fun initTaskImplementations() {
            TaskRegistry.register(TaskType.DO_WHILE.name, DoWhile::class.java)
            TaskRegistry.register(TaskType.DYNAMIC.name, Dynamic::class.java)
            TaskRegistry.register(
                TaskType.FORK_JOIN_DYNAMIC.name,
                DynamicFork::class.java
            )
            TaskRegistry.register(TaskType.FORK_JOIN.name, ForkJoin::class.java)
            TaskRegistry.register(TaskType.HTTP.name, Http::class.java)
            TaskRegistry.register(TaskType.INLINE.name, Javascript::class.java)
            TaskRegistry.register(TaskType.JOIN.name, Join::class.java)
            TaskRegistry.register(TaskType.JSON_JQ_TRANSFORM.name, JQ::class.java)
            TaskRegistry.register(TaskType.SET_VARIABLE.name, SetVariable::class.java)
            TaskRegistry.register(TaskType.SIMPLE.name, SimpleTask::class.java)
            TaskRegistry.register(TaskType.SUB_WORKFLOW.name, SubWorkflow::class.java)
            TaskRegistry.register(TaskType.SWITCH.name, Switch::class.java)
            TaskRegistry.register(TaskType.TERMINATE.name, Terminate::class.java)
            TaskRegistry.register(TaskType.WAIT.name, Wait::class.java)
            TaskRegistry.register(TaskType.EVENT.name, Event::class.java)
        }
    }
}