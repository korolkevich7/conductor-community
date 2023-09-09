package com.netflix.conductor.client.kotlin.worker;

import java.io.InputStream;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.netflix.conductor.common.config.ObjectMapperProvider;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestWorkflowTask {

    private ObjectMapper objectMapper;

    @Before
    public void setup() {
        objectMapper = new ObjectMapperProvider().getObjectMapper();
    }

    @Test
    public void test() throws Exception {
        WorkflowTask task = new WorkflowTask();
        task.setType("Hello");
        task.setName("name");

        String json = objectMapper.writeValueAsString(task);

        WorkflowTask read = objectMapper.readValue(json, WorkflowTask.class);
        assertNotNull(read);
        assertEquals(task.getName(), read.getName());
        assertEquals(task.getType(), read.getType());

        task = new WorkflowTask();
        task.setWorkflowTaskType(TaskType.SUB_WORKFLOW);
        task.setName("name");

        json = objectMapper.writeValueAsString(task);

        read = objectMapper.readValue(json, WorkflowTask.class);
        assertNotNull(read);
        assertEquals(task.getName(), read.getName());
        assertEquals(task.getType(), read.getType());
        assertEquals(TaskType.SUB_WORKFLOW.name(), read.getType());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testObjectMapper() throws Exception {
        try (InputStream stream = TestWorkflowTask.class.getResourceAsStream("/tasks.json")) {
            List<Task> tasks = objectMapper.readValue(stream, List.class);
            assertNotNull(tasks);
            assertEquals(1, tasks.size());
        }
    }
}
