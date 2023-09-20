/*
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.client.kotlin.automator

import com.netflix.conductor.client.kotlin.http.TaskClient
import com.netflix.conductor.client.kotlin.worker.Worker.Companion.create
import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.conductor.common.metadata.tasks.TaskResult
import org.mockito.Mockito
import java.util.*
import java.util.function.Function
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class TaskRunnerConfigurerTest {
    private lateinit var client: TaskClient
    @BeforeTest
    fun setup() {
        client = Mockito.mock(TaskClient::class.java)
    }

    @Test//(expected = ConductorClientException::class)
    fun testInvalidThreadConfig() {
        val worker1 = create("task1", Function<Task, TaskResult> { task: Task? -> TaskResult(task) })
        val worker2 = create("task2", Function<Task, TaskResult> { task: Task? -> TaskResult(task) })
        val taskThreadCount = mutableMapOf(worker1.taskDefName to 2, worker2.taskDefName to 3)
        TaskRunnerConfigurer {
            taskClient = client
            workers = listOf(worker1, worker2)
            this.taskThreadCount = taskThreadCount
        }
    }

    @Test
    fun testMissingTaskThreadConfig() {
        val worker1 = create("task1", Function<Task, TaskResult> { task: Task? -> TaskResult(task) })
        val worker2 = create("task2", Function<Task, TaskResult> { task: Task? -> TaskResult(task) })
        val taskThreadCount = mutableMapOf(worker1.taskDefName to 2)
        val configurer = TaskRunnerConfigurer {
            taskClient = client
            workers = listOf(worker1, worker2)
            this.taskThreadCount = taskThreadCount
        }
        assertFalse(configurer.taskThreadCount.isEmpty())
        assertEquals(2, configurer.taskThreadCount.size.toLong())
        assertEquals(2, configurer.taskThreadCount["task1"]?.toLong())
        assertEquals(1, configurer.taskThreadCount["task2"]?.toLong())
    }

    @Test
    fun testPerTaskThreadPool() {
        val worker1 = create("task1", Function<Task, TaskResult> { task: Task? -> TaskResult(task) })
        val worker2 = create("task2", Function<Task, TaskResult> { task: Task? -> TaskResult(task) })
        val taskThreadCount = mutableMapOf(worker1.taskDefName to 2, worker2.taskDefName to 3)
        val configurer = TaskRunnerConfigurer {
            taskClient = client
            this.taskThreadCount = taskThreadCount
            workers = listOf(worker1, worker2)
        }
        configurer.init()
        assertEquals(2, configurer.taskThreadCount["task1"]?.toLong())
        assertEquals(3, configurer.taskThreadCount["task2"]?.toLong())
    }

    private fun testTask(taskDefName: String): Task {
        val task = Task()
        task.taskId = UUID.randomUUID().toString()
        task.status = Task.Status.IN_PROGRESS
        task.taskDefName = taskDefName
        return task
    }

    companion object {
        private const val TEST_TASK_DEF_NAME = "test"
    }
}
