package com.netflix.conductor.client.kotlin.automator;

import com.netflix.conductor.client.kotlin.exception.ConductorClientException;
import com.netflix.conductor.client.kotlin.http.TaskClient;
import com.netflix.conductor.client.kotlin.worker.Worker;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TaskRunnerConfigurerTest {

    private static final String TEST_TASK_DEF_NAME = "test";

    private TaskClient client;

    @Before
    public void setup() {
        client = Mockito.mock(TaskClient.class);
    }

    @Test(expected = NullPointerException.class)
    public void testNoWorkersException() {
        new TaskRunnerConfigurer.Builder(null, null).build();
    }

    @Test(expected = ConductorClientException.class)
    public void testInvalidThreadConfig() {
        Worker worker1 = Worker.create("task1", TaskResult::new);
        Worker worker2 = Worker.create("task2", TaskResult::new);
        Map<String, Integer> taskThreadCount = new HashMap<>();
        taskThreadCount.put(worker1.getTaskDefName(), 2);
        taskThreadCount.put(worker2.getTaskDefName(), 3);
        new TaskRunnerConfigurer.Builder(client, Arrays.asList(worker1, worker2))
                .withThreadCount(10)
                .withTaskThreadCount(taskThreadCount)
                .build();
    }

    @Test
    public void testMissingTaskThreadConfig() {
        Worker worker1 = Worker.create("task1", TaskResult::new);
        Worker worker2 = Worker.create("task2", TaskResult::new);
        Map<String, Integer> taskThreadCount = new HashMap<>();
        taskThreadCount.put(worker1.getTaskDefName(), 2);
        TaskRunnerConfigurer configurer =
                new TaskRunnerConfigurer.Builder(client, Arrays.asList(worker1, worker2))
                        .withTaskThreadCount(taskThreadCount)
                        .build();

        assertFalse(configurer.getTaskThreadCount().isEmpty());
        assertEquals(2, configurer.getTaskThreadCount().size());
        assertEquals(2, configurer.getTaskThreadCount().get("task1").intValue());
        assertEquals(1, configurer.getTaskThreadCount().get("task2").intValue());
    }

    @Test
    public void testPerTaskThreadPool() {
        Worker worker1 = Worker.create("task1", TaskResult::new);
        Worker worker2 = Worker.create("task2", TaskResult::new);
        Map<String, Integer> taskThreadCount = new HashMap<>();
        taskThreadCount.put(worker1.getTaskDefName(), 2);
        taskThreadCount.put(worker2.getTaskDefName(), 3);
        TaskRunnerConfigurer configurer =
                new TaskRunnerConfigurer.Builder(client, Arrays.asList(worker1, worker2))
                        .withTaskThreadCount(taskThreadCount)
                        .build();
        configurer.init();
        assertEquals(-1, configurer.getThreadCount());
        assertEquals(2, configurer.getTaskThreadCount().get("task1").intValue());
        assertEquals(3, configurer.getTaskThreadCount().get("task2").intValue());
    }

    @Test
    public void testSharedThreadPool() {
        Worker worker = Worker.create(TEST_TASK_DEF_NAME, TaskResult::new);
        TaskRunnerConfigurer configurer =
                new TaskRunnerConfigurer.Builder(client, Arrays.asList(worker, worker, worker))
                        .build();
        configurer.init();
        assertEquals(3, configurer.getThreadCount());
        assertEquals(500, configurer.getSleepWhenRetry());
        assertEquals(3, configurer.getUpdateRetryCount());
        assertEquals(10, configurer.getShutdownGracePeriodSeconds());
        assertFalse(configurer.getTaskThreadCount().isEmpty());
        assertEquals(1, configurer.getTaskThreadCount().size());
        assertEquals(3, configurer.getTaskThreadCount().get(TEST_TASK_DEF_NAME).intValue());

        configurer =
                new TaskRunnerConfigurer.Builder(client, Collections.singletonList(worker))
                        .withThreadCount(100)
                        .withSleepWhenRetry(100)
                        .withUpdateRetryCount(10)
                        .withShutdownGracePeriodSeconds(15)
                        .withWorkerNamePrefix("test-worker-")
                        .build();
        assertEquals(100, configurer.getThreadCount());
        configurer.init();
        assertEquals(100, configurer.getThreadCount());
        assertEquals(100, configurer.getSleepWhenRetry());
        assertEquals(10, configurer.getUpdateRetryCount());
        assertEquals(15, configurer.getShutdownGracePeriodSeconds());
        assertEquals("test-worker-", configurer.getWorkerNamePrefix());
        assertFalse(configurer.getTaskThreadCount().isEmpty());
        assertEquals(1, configurer.getTaskThreadCount().size());
        assertEquals(100, configurer.getTaskThreadCount().get(TEST_TASK_DEF_NAME).intValue());
    }

    private Task testTask(String taskDefName) {
        Task task = new Task();
        task.setTaskId(UUID.randomUUID().toString());
        task.setStatus(Task.Status.IN_PROGRESS);
        task.setTaskDefName(taskDefName);
        return task;
    }
}
