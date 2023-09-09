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
package com.netflix.conductor.client.kotlin.worker

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.conductor.common.config.ObjectMapperProvider
import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.conductor.common.metadata.tasks.TaskType
import com.netflix.conductor.common.metadata.workflow.WorkflowTask
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.Test
import kotlin.test.BeforeTest

class TestWorkflowTask {
    private lateinit var objectMapper: ObjectMapper
    @BeforeTest
    fun setup() {
        objectMapper = ObjectMapperProvider().objectMapper
    }

    @Test
    @Throws(Exception::class)
    fun test() {
        var task = WorkflowTask()
        task.type = "Hello"
        task.name = "name"

        var json = objectMapper.writeValueAsString(task)
        var read: WorkflowTask = objectMapper.readValue(json)

        assertNotNull(read)
        assertEquals(task.name, read.name)
        assertEquals(task.type, read.type)

        task = WorkflowTask()
        task.setWorkflowTaskType(TaskType.SUB_WORKFLOW)
        task.name = "name"

        json = objectMapper.writeValueAsString(task)
        read = objectMapper.readValue(json)

        assertNotNull(read)
        assertEquals(task.name, read.name)
        assertEquals(task.type, read.type)
        assertEquals(TaskType.SUB_WORKFLOW.name, read.type)
    }

    @Test
    @Throws(Exception::class)
    fun testObjectMapper() {
        val jsonString: String = File("src/test/resources/tasks.json").readText()
        val tasks: List<Task> =
            objectMapper.readValue<List<Task>>(jsonString)
        assertNotNull(tasks)
        assertEquals(1, tasks.size.toLong())
    }
}
