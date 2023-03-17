package com.netflix.conductor.client.kotlin.config;

import com.netflix.conductor.client.kotlin.worker.Worker;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestPropertyFactory {

    @Test
    public void testIdentity() {
        Worker worker = Worker.create("Test2", TaskResult::new);
        assertNotNull(worker.getIdentity());
        boolean paused = worker.paused();
        assertFalse("Paused? " + paused, paused);
    }

    @Test
    public void test() {

        int val = PropertyFactory.getInteger("workerB", "pollingInterval", 100);
        assertEquals("got: " + val, 2, val);
        assertEquals(
                100, PropertyFactory.getInteger("workerB", "propWithoutValue", 100).intValue());

        assertFalse(
                PropertyFactory.getBoolean(
                        "workerB", "paused", true)); // Global value set to 'false'
        assertTrue(
                PropertyFactory.getBoolean(
                        "workerA", "paused", false)); // WorkerA value set to 'true'

        assertEquals(
                42,
                PropertyFactory.getInteger("workerA", "batchSize", 42)
                        .intValue()); // No global value set, so will return the default value
        // supplied
        assertEquals(
                84,
                PropertyFactory.getInteger("workerB", "batchSize", 42)
                        .intValue()); // WorkerB's value set to 84

        assertEquals("domainA", PropertyFactory.getString("workerA", "domain", null));
        assertEquals("domainB", PropertyFactory.getString("workerB", "domain", null));
        assertNull(PropertyFactory.getString("workerC", "domain", null)); // Non Existent
    }

    @Test
    public void testProperty() {
        Worker worker = Worker.create("Test", TaskResult::new);
        boolean paused = worker.paused();
        assertTrue("Paused? " + paused, paused);
    }
}
