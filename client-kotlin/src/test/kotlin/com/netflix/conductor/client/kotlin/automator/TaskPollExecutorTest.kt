package com.netflix.conductor.client.kotlin.automator

import com.netflix.appinfo.InstanceInfo
import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.client.kotlin.http.TaskClient
import com.netflix.conductor.client.kotlin.http.ktor.KtorClientTest
import com.netflix.conductor.client.kotlin.sample.SampleWorker
import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.conductor.common.metadata.tasks.TaskResult
import com.netflix.conductor.common.metadata.tasks.TaskType
import com.netflix.discovery.EurekaClient
import io.mockk.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

@OptIn(ExperimentalCoroutinesApi::class)
class TaskPollExecutorTest: KtorClientTest() {
    val taskClient = mockk<TaskClient>()
    val eurekaClient = mockk<EurekaClient>()
    val taskPoolExecutor: TaskPollExecutor

    init {
        val tasks = listOf(Task(), Task())
        taskPoolExecutor = TaskPollExecutor(eurekaClient,
            taskClient,
            updateRetryCount = 2,
            taskToDomain = emptyMap(),
            leaseExtendDispatcher = Dispatchers.IO.limitedParallelism(2))
    }

    @Test
    fun poll() = pollingTest(InstanceInfo.InstanceStatus.UP, false, 2)

    @Test
    fun pollFromPausedWorker() = pollingTest(InstanceInfo.InstanceStatus.UP, true, 0)

    @Test
    fun pollFromDownInstanceWithEureka() = pollingTest(InstanceInfo.InstanceStatus.DOWN, false, 0)

    private fun pollingTest(instanceStatus: InstanceInfo.InstanceStatus, isWorkerPaused: Boolean, expectedTasksSize: Int): Unit = runBlocking {
        every { eurekaClient.instanceRemoteStatus } returns instanceStatus

        val worker = spyk(SampleWorker("task_1"))
        every { worker.paused() } returns isWorkerPaused

        coEvery {
            taskClient.batchPollTasksInDomain("task_1", null, any(), 10, 1000)
        } returns listOf(Task(), Task())

        val tasks = taskPoolExecutor.poll(worker)

        assertEquals(tasks.size, expectedTasksSize)
    }

    @Test
    fun processTask(): Unit = runBlocking {
        val worker = spyk(SampleWorker("task_1"))
        val task = Task()
        task.taskDefName = "task_1"
        task.taskType = TaskType.SIMPLE.name
        task.startTime = System.currentTimeMillis()
        task.responseTimeoutSeconds = 10
        task.status = Task.Status.SCHEDULED
        val taskWithResult = taskPoolExecutor.processTask(worker, task)
        println("RESULT ${taskWithResult.second}")
        assertNotNull(taskWithResult)
        assertEquals(taskWithResult.second.status, TaskResult.Status.COMPLETED)
    }

    @Test
    fun updateTaskResult(): Unit = runBlocking {
        val task = Task()
        task.status = Task.Status.COMPLETED
        task.taskDefName = "task_1"
        task.taskType = TaskType.SIMPLE.name
        task.startTime = (now() - 100.milliseconds).inWholeMilliseconds
        task.endTime = now().inWholeMilliseconds
        val taskResult = TaskResult(task)
        val taskResultCapturingSlot = slot<TaskResult>()
        val path = "/externalStorage/path/task_1"

        val worker = spyk(SampleWorker("task_1"))
        coEvery { taskClient.evaluateAndUploadLargePayload(any(), task.taskType) } returns path
        coEvery { taskClient.updateTask(taskResult) } just runs
        taskPoolExecutor.updateTaskResult(task, taskResult, worker)

        coVerify(exactly = 1) { taskClient.evaluateAndUploadLargePayload(any(), task.taskType) }
        coVerify(exactly = 1) { taskClient.updateTask(capture(taskResultCapturingSlot)) }
        assertEquals(path, taskResultCapturingSlot.captured.externalOutputPayloadStoragePath)
        assertNull(taskResultCapturingSlot.captured.outputData)
    }

    @Test
    fun updateTaskResultWithRetry(): Unit = runBlocking {
        val task = Task()
        task.status = Task.Status.COMPLETED
        task.taskDefName = "task_1"
        task.taskType = TaskType.SIMPLE.name
        task.startTime = (now() - 100.milliseconds).inWholeMilliseconds
        task.endTime = now().inWholeMilliseconds
        val taskResult = TaskResult(task)

        val worker = spyk(SampleWorker("task_1"))
        coEvery { taskClient.evaluateAndUploadLargePayload(any(), task.taskType) } throws ConductorClientException("IO") andThen null
        coEvery { taskClient.updateTask(taskResult) } throws ConductorClientException("IO") andThen Unit

        taskPoolExecutor.updateTaskResult(task, taskResult, worker)

        coVerify(exactly = 2) { taskClient.evaluateAndUploadLargePayload(any(), task.taskType) }
        coVerify(exactly = 2) { taskClient.updateTask(taskResult) }
    }

    @Test
    fun failUpdateTaskResultWithRetry(): Unit = runBlocking {
        val task = Task()
        task.status = Task.Status.COMPLETED
        task.taskDefName = "task_1"
        task.taskType = TaskType.SIMPLE.name
        task.startTime = (now() - 100.milliseconds).inWholeMilliseconds
        task.endTime = now().inWholeMilliseconds
        val taskResult = TaskResult(task)

        val worker = spyk(SampleWorker("task_1"))
        coEvery { taskClient.evaluateAndUploadLargePayload(any(), task.taskType) } throwsMany listOf(
            ConductorClientException("IO"), ConductorClientException("IO")
        )
        coEvery { taskClient.updateTask(taskResult) } throws ConductorClientException("IO") andThen Unit

        taskPoolExecutor.updateTaskResult(task, taskResult, worker)

        coVerify(exactly = 2) { taskClient.evaluateAndUploadLargePayload(any(), task.taskType) }
        coVerify(exactly = 0) { taskClient.updateTask(taskResult)}
        coVerify { worker.onErrorUpdate(task) }
    }
}

fun now(): Duration = System.currentTimeMillis().milliseconds