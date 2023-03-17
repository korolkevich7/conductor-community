package com.netflix.conductor.client.kotlin.sample;

import com.netflix.conductor.client.kotlin.automator.TaskRunnerConfigurer;
import com.netflix.conductor.client.kotlin.http.TaskClient;
import com.netflix.conductor.client.kotlin.http.jersey.JerseyTaskClient;
import com.netflix.conductor.client.kotlin.worker.Worker;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {

        TaskClient taskClient = new JerseyTaskClient();
        taskClient.setRootURI("http://localhost:8080/api/"); // Point this to the server API

        int threadCount =
                2; // number of threads used to execute workers.  To avoid starvation, should be
        // same or more than number of workers

        Worker worker1 = new SampleWorker("task_1");
        Worker worker2 = new SampleWorker("task_5");

        // Create TaskRunnerConfigurer
        TaskRunnerConfigurer configurer =
                new TaskRunnerConfigurer.Builder(taskClient, Arrays.asList(worker1, worker2))
                        .withThreadCount(threadCount)
                        .build();

        // Start the polling and execution of tasks
        configurer.init();
    }
}
